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
    class BlockBlobServiceSideSyncCopyController : TransferControllerBase
    {
        /// <summary>
        /// Internal state values.
        /// </summary>
        private enum State
        {
            FetchSourceAttributes,
            GetDestination,
            Copy,
            Commit,
            Finished,
            Error,
        }

        private State state;

        private bool hasWork;

        private string BlockIdPrefix;

        private object blockIdsLock = new object();
        private AzureBlobLocation sourceLocation;
        private AzureBlobLocation destLocation;

        private CloudBlob sourceBlob;
        private CloudBlockBlob destBlob;

        private long totalLength;
        private long blockSize;

        CountdownEvent countdownEvent;
        private SortedDictionary<int, string> blockIds;

        private Queue<long> lastTransferWindow;
        private Attributes sourceAttributes = null;

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
            if (null == transferJob.Destination)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "Dest"),
                    "transferJob");
            }

            this.sourceLocation = this.TransferJob.Source as AzureBlobLocation;
            this.destLocation = this.TransferJob.Destination as AzureBlobLocation;

            this.sourceBlob = sourceLocation.Blob;
            this.destBlob = destLocation.Blob as CloudBlockBlob;

            this.state = State.FetchSourceAttributes;
            this.hasWork = true;
        }

        public override bool HasWork => this.hasWork;

        protected override async Task<bool> DoWorkInternalAsync()
        {
            if (!this.TransferJob.Transfer.ShouldTransferChecked)
            {
                this.hasWork = false;
                if (await this.CheckShouldTransfer())
                {
                    return true;
                }
                else
                {
                    this.hasWork = true;
                    return false;
                }
            }

            switch (this.state)
            {
                case State.FetchSourceAttributes:
                    await this.FetchSourceAttributesAsync();
                    break;
                case State.GetDestination:
                    await this.GetDestinationAsync();
                    break;
                case State.Copy:
                    await this.CopyBlockAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
                    break;
                case State.Finished:
                case State.Error:
                default:
                    break;
            }

            return (State.Error == this.state || State.Finished == this.state);
        }

        private async Task FetchSourceAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchSourceAttributes,
                "FetchSourceAttributesAsync called, but state isn't FetchSourceAttributes");

            this.hasWork = false;
            this.StartCallbackHandler();
            if (this.sourceLocation.IsInstanceInfoFetched != true)
            {
                AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                     this.sourceLocation.ETag,
                     this.sourceLocation.AccessCondition,
                     this.sourceLocation.CheckedAccessCondition);
                try
                {
                    await this.sourceBlob.FetchAttributesAsync(
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.TransferContext),
                        this.CancellationToken);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
                catch (StorageException e)
                {
#endif
                    HandleFetchSourceAttributesException(e);
                    throw;
                }

                this.sourceLocation.CheckedAccessCondition = true;
            }

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if (0 != this.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceBlob.Properties.ETag;
            }
            else if ((this.TransferJob.CheckPoint.EntryTransferOffset > this.sourceBlob.Properties.Length)
                 || (this.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.sourceAttributes = Utils.GenerateAttributes(this.sourceBlob);

            this.totalLength = this.sourceBlob.Properties.Length;

            if (this.IsForceOverwrite)
            {
                this.PrepareForCopy();

                // Handle record overwrite here.
                this.state = State.Copy;
            }
            else
            {
                this.state = State.GetDestination;
            }

            this.hasWork = true;
        }

        private async Task GetDestinationAsync()
        {
            this.hasWork = false;

            try
            {
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    this.destLocation.AccessCondition,
                    this.destLocation.CheckedAccessCondition);

                await this.destBlob.FetchAttributesAsync(
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

        private async Task<bool> HandleGetDestinationResultAsync(Exception e)
        {
            bool destExist = false;

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
                    //else
                    //{
                    //    if (this.sourceBlob.Properties.BlobType != this.destBlob.Properties.BlobType)
                    //    {
                    //        throw new InvalidOperationException(Resources.SourceAndDestinationBlobTypeDifferent);
                    //    }
                    //}
                    return false;
                }
            }

            this.destLocation.CheckedAccessCondition = true;
            
            // If destination file exists, query user whether to overwrite it.
            await this.CheckOverwriteAsync(
                destExist,
                this.sourceBlob.Uri.ToString(),
                this.destBlob.Uri.ToString());

            this.state = State.Copy;

            this.hasWork = true;
            return true;
        }

        private void PrepareForCopy()
        {
            this.blockSize = TransferManager.Configurations.BlockSize;

            SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;

            if (null != checkpoint.TransferWindow)
            {
                this.lastTransferWindow = new Queue<long>(checkpoint.TransferWindow);
            }

            int blockCount = null == this.lastTransferWindow ? 0 : this.lastTransferWindow.Count + (int)Math.Ceiling((double)(totalLength - checkpoint.EntryTransferOffset) / this.blockSize);
            this.countdownEvent = new CountdownEvent(blockCount);

            this.blockIds = new SortedDictionary<int, string>();
            this.BlockIdPrefix = GenerateBlockIdPrefix();
            this.InitializeBlockIds();
        }

        private void InitializeBlockIds()
        {
            int count = (int)Math.Ceiling((double)this.TransferJob.CheckPoint.EntryTransferOffset / this.blockSize);

            for (int i = 0; i < count; ++i)
            {
                this.blockIds.Add(i, GetBlockIdByIndex(i));
            }
        }

        private static void HandleFetchSourceAttributesException(StorageException e)
        {
            // Getting a storage exception is expected if the source doesn't
            // exist. For those cases that indicate the source doesn't exist
            // we will set a specific error state.
            if (e?.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException(Resources.SourceDoesNotExistException, e);
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

        private async Task CopyBlockAsync()
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

                        if (checkpoint.EntryTransferOffset < this.totalLength)
                        {
                            checkpoint.TransferWindow.Add(startOffset);
                            checkpoint.EntryTransferOffset = Math.Min(
                                checkpoint.EntryTransferOffset + this.blockSize,
                                this.totalLength);

                            canUpload = true;
                        }
                    }
                }

                if (!canUpload)
                {
                    return;
                }
            }

            hasWork = ((null != this.lastTransferWindow) || (this.TransferJob.CheckPoint.EntryTransferOffset < this.totalLength));
            string blockId = this.GetBlockIdByIndex((int)(startOffset / this.blockSize));

            if (needGenerateBlockId)
            {
                this.blockIds.Add((int)(startOffset / this.blockSize), blockId);
            }

            await Task.Yield();

            Uri sourceUri = this.sourceBlob.GenerateCopySourceUri();
            long length = Math.Min(this.totalLength - startOffset, this.blockSize);
            
            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                this.destLocation.AccessCondition,
                this.destLocation.CheckedAccessCondition);

            var operationContext = Utils.GenerateOperationContext(this.TransferContext);
            operationContext.UserHeaders = new Dictionary<string, string>(capacity: 1);
            operationContext.UserHeaders.Add(
                Shared.Protocol.Constants.HeaderConstants.SourceIfMatchHeader,
                this.sourceLocation.ETag);

            await this.destBlob.PutBlockAsync(
                blockId, 
                sourceUri, 
                startOffset, 
                length, 
                null,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                operationContext,
                this.CancellationToken);

            this.destLocation.CheckedAccessCondition = true;

            lock (checkpoint.TransferWindowLock)
            {
                checkpoint.TransferWindow.Remove(startOffset);
            }

            this.FinishBlock();
        }

        private async Task CommitAsync()
        {
            Debug.Assert(
                this.state == State.Commit,
                "CommitAsync called, but state isn't Commit",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            var sourceProperties = this.sourceBlob.Properties;

            Utils.SetAttributes(this.destBlob, 
                new Attributes()
                {
                    CacheControl = sourceProperties.CacheControl,
                    ContentDisposition = sourceProperties.ContentDisposition,
                    ContentEncoding = sourceProperties.ContentEncoding,
                    ContentLanguage = sourceProperties.ContentLanguage,
                    ContentMD5 = sourceProperties.ContentMD5,
                    ContentType = sourceProperties.ContentType,
                    Metadata = this.sourceBlob.Metadata,
                    OverWriteAll = true
                });
            await this.SetCustomAttributesAsync(this.destBlob);

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);

            await this.destBlob.PutBlockListAsync(
                        this.blockIds.Values,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken);

            // REST API PutBlockList cannot clear existing Content-Type of block blob, so if it's needed to clear existing
            // Content-Type, REST API SetBlobProperties must be called explicitly:
            // 1. The attributes are inherited from others and Content-Type is null or empty.
            // 2. User specifies Content-Type to string.Empty while uploading.
            if ((this.IsForceOverwrite && string.IsNullOrEmpty(this.sourceAttributes.ContentType))
                || (!this.IsForceOverwrite && this.sourceAttributes.ContentType == string.Empty))
            {
                await this.destBlob.SetPropertiesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
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
                var blobNameBytes = Encoding.UTF8.GetBytes(this.destBlob.Name);
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
