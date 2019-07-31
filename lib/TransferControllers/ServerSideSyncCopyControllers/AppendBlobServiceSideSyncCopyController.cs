//------------------------------------------------------------------------------
// <copyright file="AppendBlobServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.Blob.Protocol;

    /// <summary>
    /// Transfer controller to copy to append blob with AppendBlockFromURL.
    /// </summary>
    class AppendBlobServiceSideSyncCopyController : TransferControllerBase
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

        private AzureBlobLocation sourceLocation = null;
        private AzureBlobLocation destLocation = null;

        private CloudBlob sourceBlob = null;
        private CloudAppendBlob destBlob = null;

        private bool hasWork = false;

        private State state;

        private long blockSize = Constants.DefaultChunkSize;

        private long totalLength;

        private bool gotDestAttributes = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="AppendBlobServiceSideSyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal AppendBlobServiceSideSyncCopyController(
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
            this.destBlob = destLocation.Blob as CloudAppendBlob;

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

            AccessCondition accessCondition = null == this.sourceLocation.ETag ?
                Utils.GenerateConditionWithCustomerCondition(this.sourceLocation.AccessCondition, this.sourceLocation.CheckedAccessCondition) : 
                Utils.GenerateIfMatchConditionWithCustomerCondition(this.sourceLocation.ETag, this.sourceLocation.AccessCondition, this.destLocation.CheckedAccessCondition);

            try
            {
                await this.sourceBlob.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext));
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

            this.totalLength = this.sourceBlob.Properties.Length;
            this.sourceLocation.ETag = this.sourceBlob.Properties.ETag;
            this.sourceLocation.CheckedAccessCondition = true;
            // No actual data change has made yet, no need for journal updating.
            
            if (0 == this.TransferJob.CheckPoint.EntryTransferOffset)
            {
                this.state = State.GetDestination;
            }
            else
            {
                if (this.TransferJob.CheckPoint.EntryTransferOffset == this.totalLength)
                {
                    this.state = State.Commit;
                }
                else
                {
                    this.state = State.Copy;
                }
            }

            this.hasWork = true;
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

        private async Task GetDestinationAsync()
        {
            this.hasWork = false;
            bool needCreateDestination = true;
            if (!this.IsForceOverwrite)
            {
                if (this.TransferJob.Overwrite.HasValue)
                {
                    if (!this.TransferJob.Overwrite.Value)
                    {
                        await this.CheckOverwriteAsync(
                            true,
                            this.sourceBlob.Uri.ToString(),
                            this.destBlob.Uri.ToString());
                    }
                }
                else if (!this.destLocation.CheckedAccessCondition && null != this.destLocation.AccessCondition)
                {
                    try
                    {
                        await this.destBlob.FetchAttributesAsync(
                            Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, false),
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.TransferContext),
                            this.CancellationToken);

                        this.destLocation.CheckedAccessCondition = true;

                        await this.CheckOverwriteAsync(
                            true,
                            this.sourceBlob.Uri.ToString(),
                            this.destBlob.Uri.ToString());

                        this.gotDestAttributes = true;
                    }
                    catch (StorageException se)
                    {
                        if ((null == se.RequestInformation) || ((int)HttpStatusCode.NotFound != se.RequestInformation.HttpStatusCode))
                        {
                            throw;
                        }
                    }
                }
                else
                {
                    AccessCondition accessCondition = new AccessCondition();
                    accessCondition.IfNoneMatchETag = "*";

                    try
                    {
                        await this.destBlob.CreateOrReplaceAsync(
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.TransferContext));

                        needCreateDestination = false;
                        this.destLocation.CheckedAccessCondition = true;
                        this.TransferJob.Transfer.UpdateJournal();
                    }
                    catch (StorageException se)
                    {
                        if ((null != se.RequestInformation)
                            && (((int)HttpStatusCode.PreconditionFailed == se.RequestInformation.HttpStatusCode))
                                || (((int)HttpStatusCode.Conflict == se.RequestInformation.HttpStatusCode)
                                    && string.Equals(se.RequestInformation.ErrorCode, "BlobAlreadyExists")))
                        {
                            await this.CheckOverwriteAsync(
                                true,
                                this.sourceBlob.Uri.ToString(),
                                this.destBlob.Uri.ToString());
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
            else
            {
                this.gotDestAttributes = true;
            }

            Utils.CheckCancellation(this.CancellationToken);

            if (needCreateDestination)
            {
                await this.destBlob.CreateOrReplaceAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext));

                this.destLocation.CheckedAccessCondition = true;
                this.TransferJob.Transfer.UpdateJournal();
            }

            if (this.TransferJob.CheckPoint.EntryTransferOffset == this.totalLength)
            {
                this.state = State.Commit;
            }
            else
            {
                this.state = State.Copy;
            }
            this.hasWork = true;
        }

        private async Task CopyBlockAsync()
        {
            long startOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
            this.hasWork = false;

            await Task.Yield();

            long length = Math.Min(this.blockSize, this.totalLength - startOffset);

            Uri sourceUri = this.sourceBlob.GenerateCopySourceUri();

            AccessCondition sourceAccessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.sourceLocation.ETag, this.sourceLocation.AccessCondition, true);

            AccessCondition destAccessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true) ?? new AccessCondition();
            destAccessCondition.IfAppendPositionEqual = startOffset;

            bool needToCheckContent = false;
            Exception catchedStorageException = null;

            try
            {
                await this.destBlob.AppendBlockAsync(sourceUri,
                    startOffset,
                    length,
                    null,
                    sourceAccessCondition,
                    destAccessCondition,
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }
            catch (StorageException se)
            {
                if ((null != se.RequestInformation) &&
                    ((int)HttpStatusCode.PreconditionFailed == se.RequestInformation.HttpStatusCode) &&
                    (se.RequestInformation.ErrorCode == BlobErrorCodeStrings.InvalidAppendCondition))
                {
                    needToCheckContent = true;
                    catchedStorageException = se;
                }
                else
                {
                    throw;
                }
            }

            if (needToCheckContent &&
                (!await this.ValidateAppendedChunkAsync(startOffset, length)))
            {
                throw new InvalidOperationException(Resources.DestinationChangedException, catchedStorageException);
            }

            this.sourceLocation.CheckedAccessCondition = true;

            this.UpdateProgress(() =>
            {
                this.TransferJob.CheckPoint.EntryTransferOffset += length;
                this.TransferJob.Transfer.UpdateJournal();
                this.UpdateProgressAddBytesTransferred(length);
            });

            if (this.TransferJob.CheckPoint.EntryTransferOffset == this.totalLength)
            {
                this.state = State.Commit;
            }
            this.hasWork = true;
        }

        private async Task<bool> ValidateAppendedChunkAsync(long startOffset, long length)
        {
            try
            {
                string sourceConentMD5 = null;
                OperationContext sourceOperationContext = new OperationContext();
                sourceOperationContext.ResponseReceived += (sender, eventArgs) =>
                {
                    sourceConentMD5 = eventArgs.RequestInformation.ContentMd5;
                };

                string destContentMD5 = null;
                OperationContext destOperationContext = new OperationContext();
                destOperationContext.ResponseReceived += (sender, eventArgs) =>
                {
                    destContentMD5 = eventArgs.RequestInformation.ContentMd5;
                };

                Task sourceDownloadTask = this.sourceBlob.DownloadRangeToStreamAsync(
                    new FakeStream(),
                    startOffset,
                    length,
                    null,
                    new BlobRequestOptions()
                    {
                        UseTransactionalMD5 = true
                    },
                    sourceOperationContext,
                    this.CancellationToken);
                Task destDownloadTask = this.destBlob.DownloadRangeToStreamAsync(
                    new FakeStream(),
                    startOffset,
                    length,
                    null,
                    new BlobRequestOptions()
                    {
                        UseTransactionalMD5 = true
                    },
                    destOperationContext,
                    this.CancellationToken);

                await sourceDownloadTask;
                await destDownloadTask;

                return (!string.IsNullOrEmpty(sourceConentMD5)) && string.Equals(sourceConentMD5, destContentMD5);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                return false;
            }
        }

        private async Task CommitAsync()
        {
            Debug.Assert(State.Commit == this.state, "Calling CommitAsync, state should be Commit");

            this.hasWork = false;

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);

            if (this.gotDestAttributes)
            {
                await Utils.ExecuteXsclApiCallAsync(
                    async () => await this.destBlob.FetchAttributesAsync(
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken),
                    this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.destBlob.Metadata);
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

            await this.destBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                operationContext,
                this.CancellationToken);

            if (!originalMetadata.DictionaryEquals(this.destBlob.Metadata))
            {
                await this.destBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
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

        private void SetFinish()
        {
            this.state = State.Finished;
            this.FinishCallbackHandler(null);
            this.hasWork = true;
        }

        class FakeStream : Stream
        {
            private long position = 0;
            public override void SetLength(long value)
            {
            }

            public override bool CanSeek => false;
            public override bool CanRead  => true;
            public override bool CanWrite => true;

            public override long Position
            {
                set
                {
                    this.position = value;
                }

                get
                {
                    return this.position;
                }
            }

            public override long Length => throw new NotImplementedException();

            public override void Write(byte[] buffer, int offset, int count)
            {
                this.position += count;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return 0;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void Flush()
            {
            }
        }
    }
}
