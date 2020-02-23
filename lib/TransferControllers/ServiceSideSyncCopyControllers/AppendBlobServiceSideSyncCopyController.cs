//------------------------------------------------------------------------------
// <copyright file="AppendBlobServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
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
    class AppendBlobServiceSideSyncCopyController : ServiceSideSyncCopyController
    {
        private CloudAppendBlob destAppendBlob = null;

        private long blockSize = Constants.DefaultTransferChunkSize;
        private AzureBlobLocation destLocation;

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
            this.destAppendBlob = this.destLocation.Blob as CloudAppendBlob;
            this.DestHandler = new ServiceSideSyncCopyDest.AppendBlobDestHandler(this.destLocation, transferJob);
            this.hasWork = true;
        }

        protected override void PostFetchSourceAttributes()
        {
            if (this.SourceHandler.TotalLength > Constants.MaxAppendBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SourceHandler.TotalLength),
                            Resources.AppendBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxAppendBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            if (0 == this.TransferJob.CheckPoint.EntryTransferOffset)
            {
                this.state = State.GetDestination;
            }
            else
            {
                if (this.TransferJob.CheckPoint.EntryTransferOffset == this.SourceHandler.TotalLength)
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

        protected override async Task GetDestinationAsync()
        {
            this.hasWork = false;
            await this.CheckAndCreateDestinationAsync();

            if (this.TransferJob.CheckPoint.EntryTransferOffset == this.SourceHandler.TotalLength)
            {
                this.state = State.Commit;
            }
            else
            {
                this.state = State.Copy;
            }
            this.hasWork = true;
        }

        protected override async Task CopyChunkAsync()
        {
            long startOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
            this.hasWork = false;

            await Task.Yield();

            long length = Math.Min(this.blockSize, this.SourceHandler.TotalLength - startOffset);

            Uri sourceUri = this.SourceHandler.GetCopySourceUri();

            AccessCondition sourceAccessCondition = this.SourceHandler.NeedToCheckAccessCondition 
                ? Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.SourceHandler.ETag, this.SourceHandler.AccessCondition, true)
                : null;

            AccessCondition destAccessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true) ?? new AccessCondition();
            destAccessCondition.IfAppendPositionEqual = startOffset;

            bool needToCheckContent = false;
            Exception catchedStorageException = null;

            try
            {
                await this.destAppendBlob.AppendBlockAsync(sourceUri,
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

            this.UpdateProgress(() =>
            {
                this.TransferJob.CheckPoint.EntryTransferOffset += length;
                this.TransferJob.Transfer.UpdateJournal();
                this.UpdateProgressAddBytesTransferred(length);
            });

            if (this.TransferJob.CheckPoint.EntryTransferOffset == this.SourceHandler.TotalLength)
            {
                this.state = State.Commit;
            }
            this.hasWork = true;
        }

        protected override Task DoPreCopyAsync()
        {
            throw new NotImplementedException();
        }

        private async Task<bool> ValidateAppendedChunkAsync(long startOffset, long length)
        {
            await this.destAppendBlob.FetchAttributesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            this.gotDestAttributes = true;

            if (this.destAppendBlob.Properties.Length != (startOffset + length))
            {
                return false;
            }

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

                Task[] downloadTasks = new Task[2];

                downloadTasks[0] = this.SourceHandler.DownloadRangeToStreamAsync(
                    new FakeStream(),
                    startOffset,
                    length,
                    null,
                    true,
                    sourceOperationContext,
                    this.CancellationToken);

                downloadTasks[1] = this.destAppendBlob.DownloadRangeToStreamAsync(
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

                await Task.WhenAll(downloadTasks);
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

        protected override async Task CommitAsync()
        {
            Debug.Assert(State.Commit == this.state, "Calling CommitAsync, state should be Commit");

            this.hasWork = false;

            await this.CommonCommitAsync();

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
