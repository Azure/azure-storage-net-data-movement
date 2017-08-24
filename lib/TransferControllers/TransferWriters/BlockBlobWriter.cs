//------------------------------------------------------------------------------
// <copyright file="BlockBlobWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal sealed class BlockBlobWriter : TransferReaderWriterBase
    {
        private volatile bool hasWork;
        private volatile State state;
        private CountdownEvent countdownEvent;
        private string[] blockIdSequence;
        private AzureBlobLocation destLocation;
        private CloudBlockBlob blockBlob;

        public BlockBlobWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.SharedTransferData.TransferJob.Destination as AzureBlobLocation;
            this.blockBlob = this.destLocation.Blob as CloudBlockBlob;

            Debug.Assert(null != this.blockBlob, "The destination is not a block blob while initializing a BlockBlobWriter instance.");

            this.state = State.FetchAttributes;
            this.hasWork = true;
        }

        private enum State
        {
            FetchAttributes,
            UploadBlob,
            Commit,
            Error,
            Finished
        };

        public override bool PreProcessed
        {
            get;
            protected set;
        }

        public override bool HasWork
        {
            get
            {
                return this.hasWork && 
                    (!this.PreProcessed
                    || ((this.state == State.UploadBlob) && this.SharedTransferData.AvailableData.Any())
                    || ((this.state == State.Commit) && (null != this.SharedTransferData.Attributes)));
            }
        }

        public override bool IsFinished
        {
            get
            {
                return State.Error == this.state || State.Finished == this.state;
            }
        }

        public override async Task DoWorkInternalAsync()
        {
            switch (this.state)
            {
                case State.FetchAttributes:
                    await this.FetchAttributesAsync();
                    break;
                case State.UploadBlob:
                    await this.UploadBlobAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
                    break;
                case State.Error:
                default:
                    break;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                if (null != this.countdownEvent)
                {
                    this.countdownEvent.Dispose();
                    this.countdownEvent = null;
                }
            }
        }

        private async Task FetchAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchAttributes,
                "FetchAttributesAsync called, but state isn't FetchAttributes", 
                "Current state is {0}",
                this.state);
                        
            this.hasWork = false;

            if (this.SharedTransferData.TotalLength > Constants.MaxBlockBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SharedTransferData.TotalLength),
                            Resources.BlockBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxBlockBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            if (!this.Controller.IsForceOverwrite)
            {
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    this.destLocation.AccessCondition,
                    this.destLocation.CheckedAccessCondition);

                try
                {
                    await this.destLocation.Blob.FetchAttributesAsync(
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.Controller.TransferContext),
                        this.CancellationToken);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception e) when (e is StorageException || e.InnerException is StorageException)
                {
                    var se = e as StorageException ?? e.InnerException as StorageException;
#else
                catch (StorageException se)
                {
#endif
                    this.HandleFetchAttributesResult(se);
                    return;
                }
            }

            this.HandleFetchAttributesResult(null);
        }

        private void HandleFetchAttributesResult(Exception e)
        {
            bool existingBlob = !this.Controller.IsForceOverwrite;

            if (null != e)
            {
                StorageException se = e as StorageException;

                if (null != se)
                {
                    // Getting a storage exception is expected if the blob doesn't
                    // exist. In this case we won't error out, but set the 
                    // existingBlob flag to false to indicate we're uploading
                    // a new blob instead of overwriting an existing blob.
                    if (null != se.RequestInformation &&
                        se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        existingBlob = false;
                    }
                    else if (null != se &&
                        (0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase)))
                    {
                        throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
                    }
                    else
                    {
                        throw se;
                    }
                }
                else
                {
                    throw e;
                }
            }

            this.destLocation.CheckedAccessCondition = true;

            if (string.IsNullOrEmpty(this.destLocation.BlockIdPrefix))
            {
                // BlockIdPrefix is never set before that this is the first time to transfer this file.
                // In block blob upload, it stores uploaded but not committed blocks on Azure Storage. 
                // In DM, we use block id to identify the blocks uploaded so we only need to upload it once.
                // Keep BlockIdPrefix in upload job object for restarting the transfer if anything happens.
                this.destLocation.BlockIdPrefix = GenerateBlockIdPrefix();
            }

            if (!this.Controller.IsForceOverwrite)
            {
                // If destination file exists, query user whether to overwrite it.
                this.Controller.CheckOverwrite(
                    existingBlob,
                    this.SharedTransferData.TransferJob.Source.Instance,
                    this.destLocation.Blob);
            }

            this.Controller.UpdateProgressAddBytesTransferred(0);

            if (existingBlob)
            {
                if (this.destLocation.Blob.Properties.BlobType == BlobType.Unspecified)
                {
                    throw new InvalidOperationException(Resources.FailedToGetBlobTypeException);
                }
                if (this.destLocation.Blob.Properties.BlobType != BlobType.BlockBlob)
                {
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch);
                }

                Debug.Assert(
                    this.destLocation.Blob.Properties.BlobType == BlobType.BlockBlob,
                    "BlobType should be BlockBlob if we reach here.");
            }

            // Calculate number of blocks.
            int numBlocks = (int)Math.Ceiling(
                this.SharedTransferData.TotalLength / (double)this.SharedTransferData.BlockSize);

            // Create sequence array.
            this.blockIdSequence = new string[numBlocks];

            for (int i = 0; i < numBlocks; ++i)
            {
                string blockIdSuffix = i.ToString("D6", CultureInfo.InvariantCulture);
                byte[] blockIdInBytes = System.Text.Encoding.UTF8.GetBytes(this.destLocation.BlockIdPrefix + blockIdSuffix);
                string blockId = Convert.ToBase64String(blockIdInBytes);
                this.blockIdSequence[i] = blockId;
            }

            SingleObjectCheckpoint checkpoint = this.SharedTransferData.TransferJob.CheckPoint;

            int leftBlockCount = (int)Math.Ceiling(
                (this.SharedTransferData.TotalLength - checkpoint.EntryTransferOffset) / (double)this.SharedTransferData.BlockSize) + checkpoint.TransferWindow.Count;

            if (0 == leftBlockCount)
            {
                this.state = State.Commit;
            }
            else
            {
                this.countdownEvent = new CountdownEvent(leftBlockCount);

                this.state = State.UploadBlob;
            }

            this.PreProcessed = true;
            this.hasWork = true;
        }

        private string GenerateBlockIdPrefix()
        {
            //var blockIdPrefix = Guid.NewGuid().ToString("N") + "-";

            // Originally the blockId is an GUID + "-". It will cause some problem when switch machines or jnl get cleaned
            // to upload to the same block blob - block id is not shared between the 2 DMLib instances
            // and it may result in reaching the limitation of maximum 50000 uncommited blocks + 50000 committed blocks.
            //
            // Change it to hash based prefix to make it preditable and can be shared between multiple DMLib instances
            string blobNameHash;
            using (var md5 = new MD5Wrapper())
            {
                var blobNameBytes = Encoding.UTF8.GetBytes(this.destLocation.Blob.Name);
                md5.UpdateHash(blobNameBytes, 0, blobNameBytes.Length);
                blobNameHash = md5.ComputeHash();
            }

            // The original GUID format prefix's length is 32 + 1 ("-")
            // As the service requires the blockid has the same size of each block,
            // To keep the compatibility, add 9 chars to the end of the hash ( 33 - 24)
            var blockIdPrefix = blobNameHash + "12345678-";
            return blockIdPrefix;
        }

        private async Task UploadBlobAsync()
        {
            Debug.Assert(
                State.UploadBlob == this.state || State.Error == this.state,
                "UploadBlobAsync called but state is not UploadBlob nor Error.",
                "Current state is {0}",
                this.state);

            TransferData transferData = this.GetFirstAvailable();

            if (null != transferData)
            {
                using (transferData)
                {
                    if (transferData.MemoryBuffer.Length == 1)
                    {
                        transferData.Stream = new MemoryStream(transferData.MemoryBuffer[0], 0, transferData.Length);
                    }
                    else
                    {
                        transferData.Stream = new ChunkedMemoryStream(transferData.MemoryBuffer, 0, transferData.Length);
                    }

                    await this.blockBlob.PutBlockAsync(
                        this.GetBlockId(transferData.StartOffset),
                        transferData.Stream,
                        null,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                        Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.Controller.TransferContext),
                        this.CancellationToken);
                }

                this.Controller.UpdateProgress(() =>
                {
                    lock (this.SharedTransferData.TransferJob.CheckPoint.TransferWindowLock)
                    {
                        this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Remove(transferData.StartOffset);
                    }
                    this.SharedTransferData.TransferJob.Transfer.UpdateJournal();

                    // update progress
                    this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                });

                this.FinishBlock();
            }

            // Do not set hasWork to true because it's always true in State.UploadBlob
            // Otherwise it may cause CommitAsync be called multiple times:
            //     1. UploadBlobAsync downloads all content, but doesn't set hasWork to true yet
            //     2. Call CommitAysnc, set hasWork to false
            //     3. UploadBlobAsync set hasWork to true.
            //     4. Call CommitAsync again since hasWork is true.
        }

        private async Task CommitAsync()
        {
            Debug.Assert(
                this.state == State.Commit,
                "CommitAsync called, but state isn't Commit",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            Utils.SetAttributes(this.blockBlob, this.SharedTransferData.Attributes);
            await this.Controller.SetCustomAttributesAsync(this.blockBlob);

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            await this.blockBlob.PutBlockListAsync(
                        this.blockIdSequence,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken);

            // REST API PutBlockList cannot clear existing Content-Type of block blob, so if it's needed to clear existing
            // Content-Type, REST API SetBlobProperties must be called explicitly:
            // 1. The attributes are inherited from others and Content-Type is null or empty.
            // 2. User specifies Content-Type to string.Empty while uploading.
            if ((this.SharedTransferData.Attributes.OverWriteAll && string.IsNullOrEmpty(this.blockBlob.Properties.ContentType))
                || (!this.SharedTransferData.Attributes.OverWriteAll && this.blockBlob.Properties.ContentType == string.Empty))
            {
                await this.blockBlob.SetPropertiesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
        }

        private void SetFinish()
        {
            this.state = State.Finished;
            this.NotifyFinished(null);
            this.hasWork = false;
        }

        private void FinishBlock()
        {
            Debug.Assert(
                this.state == State.UploadBlob || this.state == State.Error,
                "FinishBlock called, but state isn't Upload or Error",
                "Current state is {0}",
                this.state);

            // If a parallel operation caused the controller to be placed in
            // error state exit, make sure not to accidentally change it to
            // the Commit state.
            if (this.state == State.Error)
            {
                return;
            }

            if (this.countdownEvent.Signal())
            {
                this.state = State.Commit;
            }
        }

        private string GetBlockId(long startOffset)
        {
            Debug.Assert(startOffset % this.SharedTransferData.BlockSize == 0, "Block startOffset should be multiples of block size.");

            int count = (int)(startOffset / this.SharedTransferData.BlockSize);
            return this.blockIdSequence[count];
        }
    }
}
