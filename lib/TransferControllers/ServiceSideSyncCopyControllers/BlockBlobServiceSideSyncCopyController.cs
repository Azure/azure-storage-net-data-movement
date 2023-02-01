//------------------------------------------------------------------------------
// <copyright file="BlockBlobServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Transfer controller to copy to block blob with PutBlockFromURL.
    /// </summary>
    class BlockBlobServiceSideSyncCopyController : ServiceSideSyncCopyController
    {
        private string BlockIdPrefix;

        private AzureBlobLocation destLocation;
        private CloudBlockBlob destBlockBlob;
        private long blockSize;

        CountdownEvent countdownEvent;
        private SortedDictionary<int, string> blockIds;

        private Queue<long> lastTransferWindow;
        private bool gotDestinationAttributes = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockBlobServiceSideSyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal BlockBlobServiceSideSyncCopyController(
            TransferScheduler scheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(scheduler, transferJob, userCancellationToken)
        {
            TransferLocation sourceLocation = transferJob.Source;
            if (sourceLocation.Type == TransferLocationType.AzureBlob)
            {
                var blobLocation = sourceLocation as AzureBlobLocation;
                this.SourceHandler = new ServiceSideSyncCopySource.BlobSourceHandler(blobLocation, transferJob);
            }
            else if (sourceLocation.Type == TransferLocationType.AzureFile)
            {
                this.SourceHandler = new ServiceSideSyncCopySource.FileSourceHandler(sourceLocation as AzureFileLocation, transferJob);
            }
            else
            {
                throw new ArgumentException(
                    Resources.OnlySupportBlobAzureFileSource,
                    "transferJob");
            }

            this.destLocation = transferJob.Destination as AzureBlobLocation;
            this.destBlockBlob = this.destLocation.Blob as CloudBlockBlob;
            this.DestHandler = new ServiceSideSyncCopyDest.BlockBlobDestHandler(this.destLocation, transferJob);
            this.hasWork = true;
        }

        protected override void PostFetchSourceAttributes()
        {
            if (this.SourceHandler.TotalLength > Constants.MaxBlockBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SourceHandler.TotalLength),
                            Resources.BlockBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxBlockBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            if (this.IsForceOverwrite && null == this.destLocation.AccessCondition)
            {
                PrepareForCopy();
            }
            else
            {
                this.state = State.GetDestination;
            }

            this.hasWork = true;
        }

        protected override async Task GetDestinationAsync()
        {
            this.hasWork = false;

            try
            {
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    this.destLocation.AccessCondition,
                    this.destLocation.CheckedAccessCondition);

                await this.destBlockBlob.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
                {
                    var se = e as StorageException ?? e.InnerException as StorageException;
#else
            catch (StorageException se)
            {
#endif
                if (!await this.HandleGetDestinationResultAsync(se))
                {
                    throw;
                }
                return;
            }

            await this.HandleGetDestinationResultAsync(null);
        }

        protected override async Task CopyChunkAsync()
        {
            long startOffset = -1;
            bool needGenerateBlockId = true;

            if (null != this.lastTransferWindow)
            {
                try
                {
                    startOffset = this.lastTransferWindow.Dequeue();
                    needGenerateBlockId = false;
                }
                catch (InvalidOperationException)
                {
                    this.lastTransferWindow = null;
                }
            }

            var checkpoint = this.TransferJob.CheckPoint;

            if (-1 == startOffset)
            {
                bool canUpload = false;

                lock (checkpoint.TransferWindowLock)
                {
                    if (checkpoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                    {
                        startOffset = checkpoint.EntryTransferOffset;

                        if (checkpoint.EntryTransferOffset < this.SourceHandler.TotalLength)
                        {
                            checkpoint.TransferWindow.Add(startOffset);
                            checkpoint.EntryTransferOffset = Math.Min(
                                checkpoint.EntryTransferOffset + this.blockSize,
                                this.SourceHandler.TotalLength);

                            canUpload = true;
                        }
                    }
                }

                if (!canUpload)
                {
                    return;
                }
            }

            hasWork = ((null != this.lastTransferWindow) || (this.TransferJob.CheckPoint.EntryTransferOffset < this.SourceHandler.TotalLength));
            string blockId = this.GetBlockIdByIndex((int)(startOffset / this.blockSize));

            if (needGenerateBlockId)
            {
                this.blockIds.Add((int)(startOffset / this.blockSize), blockId);
            }

            await Task.Yield();

            Uri sourceUri = this.SourceHandler.GetCopySourceUri();
            long length = Math.Min(this.SourceHandler.TotalLength - startOffset, this.blockSize);

            AccessCondition accessCondition = this.SourceHandler.NeedToCheckAccessCondition 
                ? Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true)
                : null;

            var operationContext = Utils.GenerateOperationContext(this.TransferContext);

            if (this.SourceHandler.NeedToCheckAccessCondition)
            {
                operationContext.UserHeaders = new Dictionary<string, string>(capacity: 1);
                operationContext.UserHeaders.Add(
                    Shared.Protocol.Constants.HeaderConstants.SourceIfMatchHeader,
                    this.SourceHandler.ETag);
            }

            await this.destBlockBlob.PutBlockAsync(
                blockId,
                sourceUri,
                startOffset,
                length,
                null,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                operationContext,
                this.CancellationToken);

            this.UpdateProgress(() =>
            {
                lock (checkpoint.TransferWindowLock)
                {
                    checkpoint.TransferWindow.Remove(startOffset);
                }
                this.TransferJob.Transfer.UpdateJournal();

                this.UpdateProgressAddBytesTransferred(length);
            });

            this.FinishBlock();
        }

        protected override async Task CommitAsync()
        {
            Debug.Assert(
                this.state == State.Commit,
                "CommitAsync called, but state isn't Commit",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            Attributes sourceAttributes = this.SourceHandler.SourceAttributes;

            Utils.SetAttributes(this.destBlockBlob,
                new Attributes()
                {
                    CacheControl = sourceAttributes.CacheControl,
                    ContentDisposition = sourceAttributes.ContentDisposition,
                    ContentEncoding = sourceAttributes.ContentEncoding,
                    ContentLanguage = sourceAttributes.ContentLanguage,
                    ContentMD5 = sourceAttributes.ContentMD5,
                    ContentType = sourceAttributes.ContentType,
                    Metadata = sourceAttributes.Metadata,
                    OverWriteAll = true
                });

            await this.SetCustomAttributesAsync(this.TransferJob.Source.Instance, this.destBlockBlob);

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);

            await this.destBlockBlob.PutBlockListAsync(
                        this.blockIds.Values,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken);

            // REST API PutBlockList cannot clear existing Content-Type of block blob, so if it's needed to clear existing
            // Content-Type, REST API SetBlobProperties must be called explicitly:
            // 1. The attributes are inherited from others and Content-Type is null or empty.
            // 2. User specifies Content-Type to string.Empty while uploading.
            if ((this.gotDestinationAttributes && string.IsNullOrEmpty(this.SourceHandler.SourceAttributes.ContentType))
                || (!this.gotDestinationAttributes && this.SourceHandler.SourceAttributes.ContentType == string.Empty))
            {
                await this.destBlockBlob.SetPropertiesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
        }

        private async Task<bool> HandleGetDestinationResultAsync(Exception e)
        {
            bool destExist = true;

            if (null != e)
            {
                StorageException se = e as StorageException;

                // Getting a storage exception is expected if the destination doesn't
                // exist. In this case we won't error out, but set the 
                // destExist flag to false to indicate we will copy to 
                // a new blob/file instead of overwriting an existing one.
                if (null != se &&
                    null != se.RequestInformation &&
                    se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    destExist = false;
                }
                else
                {
                    if (null != se)
                    {
                        if (0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase))
                        {
                            // Current use error message to decide whether it caused by blob type mismatch,
                            // We should ask xscl to expose an error code for this..
                            // Opened workitem 1487579 to track this.
                            throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
                        }
                    }
                    return false;
                }
            }

            this.destLocation.CheckedAccessCondition = true;

            if (!this.IsForceOverwrite)
            {
                // If destination file exists, query user whether to overwrite it.
                await this.CheckOverwriteAsync(
                    destExist,
                    this.SourceHandler.Uri.ToString(),
                    this.destBlockBlob.Uri.ToString());
            }

            this.gotDestinationAttributes = true;
            PrepareForCopy();

            this.hasWork = true;
            return true;
        }

        private void PrepareForCopy()
        {
            var calculatedBlockSize =  ((long)Math.Ceiling((double)this.SourceHandler.TotalLength / 50000 / Constants.DefaultTransferChunkSize)) * Constants.DefaultTransferChunkSize;
            this.blockSize = Math.Max(TransferManager.Configurations.BlockSize, calculatedBlockSize);

            SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;

            if (null != checkpoint.TransferWindow)
            {
                this.lastTransferWindow = new Queue<long>(checkpoint.TransferWindow);
            }

            this.blockIds = new SortedDictionary<int, string>();
            this.BlockIdPrefix = GenerateBlockIdPrefix();
            this.InitializeBlockIds();
            int blockCount = null == this.lastTransferWindow ? 0 : this.lastTransferWindow.Count + (int)Math.Ceiling((double)(this.SourceHandler.TotalLength - checkpoint.EntryTransferOffset) / this.blockSize);

            if (0 == blockCount)
            {
                this.state = State.Commit;
            }
            else
            {
                this.countdownEvent = new CountdownEvent(blockCount);
                // Handle record overwrite here.
                this.state = State.Copy;
            }
        }

        private void InitializeBlockIds()
        {
            int count = (int)Math.Ceiling((double)this.TransferJob.CheckPoint.EntryTransferOffset / this.blockSize);

            for (int i = 0; i < count; ++i)
            {
                this.blockIds.Add(i, GetBlockIdByIndex(i));
            }
        }

        /// <summary>
        /// Sets the state of the controller to Error, while recording
        /// the last occurred exception and setting the HasWork and 
        /// IsFinished fields.
        /// </summary>
        /// <param name="ex">Exception to record.</param>
        protected override void SetErrorState(Exception ex)
        {
            Debug.Assert(
                this.state != State.Finished,
                "SetErrorState called, while controller already in Finished state");

            this.state = State.Error;
            this.hasWork = false;
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

        protected override Task DoPreCopyAsync()
        {
            throw new NotImplementedException();
        }

        private void SetFinish()
        {
            this.state = State.Finished;
            this.FinishCallbackHandler(null);
            this.hasWork = false;
        }

        private void FinishBlock()
        {
            //Debug.Assert(
            //    this.state == State.UploadBlob || this.state == State.Error,
            //    "FinishBlock called, but state isn't Upload or Error",
            //    "Current state is {0}",
            //    this.state);

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
                this.hasWork = true;
            }
        }

        private string GetBlockIdByIndex(int index)
        {
            string blockIdSuffix = index.ToString("D6", CultureInfo.InvariantCulture);
            byte[] blockIdInBytes = System.Text.Encoding.UTF8.GetBytes(this.BlockIdPrefix + blockIdSuffix);
            string blockId = Convert.ToBase64String(blockIdInBytes);

            return blockId;
        }

        private string GenerateBlockIdPrefix()
        {
            // var blockIdPrefix = Guid.NewGuid().ToString("N") + "-";

            // Originally the blockId is an GUID + "-". It will cause some problem when switch machines or jnl get cleaned
            // to upload to the same block blob - block id is not shared between the 2 DMLib instances
            // and it may result in reaching the limitation of maximum 50000 uncommited blocks + 50000 committed blocks.
            // Change it to hash based prefix to make it preditable and can be shared between multiple DMLib instances
            string blobNameHash;
            using (var md5 = new MD5Wrapper())
            {
                var blobNameBytes = Encoding.UTF8.GetBytes(this.destBlockBlob.Name);
                md5.UpdateHash(blobNameBytes, 0, blobNameBytes.Length);
                blobNameHash = md5.ComputeHash();
            }

            // The original GUID format prefix's length is 32 + 1 ("-")
            // As the service requires the blockid has the same size of each block,
            // To keep the compatibility, add 9 chars to the end of the hash ( 33 - 24)
            var blockIdPrefix = blobNameHash + "12345678-";
            return blockIdPrefix;
        }
    }
}
