//------------------------------------------------------------------------------
// <copyright file="PageBlobServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Transfer controller to copy to page blob with PutPageFromURL.
    /// </summary>
    class PageBlobServiceSideSyncCopyController :  TransferControllerBase
    {
        /// <summary>
        /// Internal state values.
        /// </summary>
        private enum State
        {
            FetchSourceAttributes,
            GetDestination,
            GetRanges,
            Copy,
            Commit,
            Finished,
            Error,
        }

        private State state;

        private bool hasWork;
        
        private AzureBlobLocation sourceLocation;
        private AzureBlobLocation destLocation;

        private CloudBlob sourceBlob;
        private CloudPageBlob sourcePageBlob;
        private CloudPageBlob destBlob;

        private long totalLength;

        CountdownEvent countdownEvent;

        private Queue<long> lastTransferWindow;
        private Attributes sourceAttributes = null;

        private long nextRangesSpanOffset = 0;
        private CountdownEvent getRangesCountdownEvent = null;

        private ConcurrentBag<List<long>> pageListBag = null;
        private List<long> pagesToCopy = null;
        private IEnumerator<long> nextPageToCopy = null;
        private bool hasNextPage = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="PageBlobServiceSideSyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal PageBlobServiceSideSyncCopyController(
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
            this.destBlob = destLocation.Blob as CloudPageBlob;

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
                case State.GetRanges:
                    await this.GetRangesAsync();
                    break;
                case State.Copy:
                    await this.CopyPageAsync();
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

            if (this.sourceBlob.BlobType == BlobType.PageBlob)
            {
                this.sourcePageBlob = this.sourceBlob as CloudPageBlob;
                this.pagesToCopy = new List<long>();
                this.pageListBag = new ConcurrentBag<List<long>>();

                if (0 == this.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    this.state = State.GetDestination;
                }
                else
                {
                    this.nextRangesSpanOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
                    if (this.nextRangesSpanOffset == this.totalLength)
                    {
                        this.PrepareForCopy();

                        if (null == this.lastTransferWindow)
                        {
                            this.state = State.Commit;
                        }
                        else
                        {
                            this.state = State.Copy;
                        }
                    }
                    else
                    {
                        this.PrepareForGetRanges();
                        this.state = State.GetRanges;
                    }
                }
            }
            else
            {
                this.PrepareForCopy();
                if (0 == this.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    this.state = State.GetDestination;
                }
                else
                {
                    this.state = State.Copy;
                }
            }

            this.hasWork = true;
        }

        private void PrepareForGetRanges()
        {
            int rangeSpanCount = (int)Math.Ceiling(((double)(this.totalLength - this.nextRangesSpanOffset)) / Constants.PageRangesSpanSize);
            this.getRangesCountdownEvent = new CountdownEvent(rangeSpanCount);
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
                else
                {
                    AccessCondition accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");
                    try
                    {
                        await this.destBlob.CreateAsync(
                            this.totalLength,
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.TransferContext));

                        needCreateDestination = false;
                        this.destLocation.CheckedAccessCondition = true;
                        this.TransferJob.Transfer.UpdateJournal();
                    }
                    catch (StorageException se)
                    {
                        if ((null != se.RequestInformation) && ((int)HttpStatusCode.PreconditionFailed == se.RequestInformation.HttpStatusCode))
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

            Utils.CheckCancellation(this.CancellationToken);

            if (needCreateDestination)
            {
                await this.destBlob.CreateAsync(
                    this.totalLength,
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
                this.TransferJob.Transfer.UpdateJournal();
            }

            await this.destBlob.ClearPagesAsync(0, this.totalLength,
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);

            if (null != this.sourcePageBlob)
            {
                this.PrepareForGetRanges();
                this.state = State.GetRanges;
            }
            else
            {
                this.state = State.Copy;
            }

            this.hasWork = true;
        }

        private async Task GetRangesAsync()
        {
            this.hasWork = false;
            long rangeSpanOffset = this.nextRangesSpanOffset;
            long rangeSpanLength = Math.Min(Constants.PageRangesSpanSize, this.totalLength - rangeSpanOffset);

            this.nextRangesSpanOffset += Constants.PageRangesSpanSize;
            this.hasWork = (this.nextRangesSpanOffset < this.totalLength);
            
            var pageRanges = await this.sourcePageBlob.GetPageRangesAsync(
                rangeSpanOffset,
                rangeSpanLength,
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            long pageOffset = rangeSpanOffset;
            List<long> pageList = new List<long>();
            foreach (var pageRange in pageRanges)
            {
                if (pageOffset <= pageRange.StartOffset)
                {
                    while (pageOffset + Constants.DefaultBlockSize < pageRange.StartOffset)
                    {
                        pageOffset += Constants.DefaultBlockSize;
                    }

                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultBlockSize;
                }

                // pageOffset > pageRange.StartOffset
                while (pageOffset < pageRange.EndOffset)
                {
                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultBlockSize;
                }
            }

            this.pageListBag.Add(pageList);

            if (this.getRangesCountdownEvent.Signal())
            {
                foreach (var pageListInARange in this.pageListBag)
                {
                    this.pagesToCopy.AddRange(pageListInARange);
                }

                this.pagesToCopy.Sort();
                this.nextPageToCopy = this.pagesToCopy.GetEnumerator();
                this.hasNextPage = this.nextPageToCopy.MoveNext();

                int pageLength = TransferManager.Configurations.BlockSize;

                SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;
                if ((null != checkpoint.TransferWindow)
                    && (0 != checkpoint.TransferWindow.Count))
                {
                    this.lastTransferWindow = new Queue<long>(checkpoint.TransferWindow);
                }

                int blockCount = null == this.lastTransferWindow ? 0 : this.lastTransferWindow.Count;
                blockCount += this.pagesToCopy.Count;

                if (0 == blockCount)
                {
                    this.state = State.Commit;
                }
                else
                {
                    this.countdownEvent = new CountdownEvent(blockCount);
                    this.state = State.Copy;
                }

                this.hasWork = true;
            }
        }

        private void PrepareForCopy()
        {
            int pageLength = TransferManager.Configurations.BlockSize;

            SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;

            if ((null != checkpoint.TransferWindow)
                && (0 != checkpoint.TransferWindow.Count))
            {
                this.lastTransferWindow = new Queue<long>(checkpoint.TransferWindow);
            }

            int blockCount = (null == this.lastTransferWindow ? 0 : this.lastTransferWindow.Count) 
                + (int)Math.Ceiling((double)(totalLength - checkpoint.EntryTransferOffset) / pageLength);
            this.countdownEvent = new CountdownEvent(blockCount);
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

                if (null != this.getRangesCountdownEvent)
                {
                    this.getRangesCountdownEvent.Dispose();
                    this.getRangesCountdownEvent = null;
                }
            }
        }

        private async Task CopyPageAsync()
        {
            long startOffset = -1;

            if (null != this.lastTransferWindow)
            {
                try
                {
                    startOffset = this.lastTransferWindow.Dequeue();
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

                if (null == this.pagesToCopy)
                {
                    // Source blob is not page blob
                    lock (checkpoint.TransferWindowLock)
                    {
                        if (checkpoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                        {
                            startOffset = checkpoint.EntryTransferOffset;

                            if (checkpoint.EntryTransferOffset < this.totalLength)
                            {
                                checkpoint.TransferWindow.Add(startOffset);
                                checkpoint.EntryTransferOffset = Math.Min(
                                    checkpoint.EntryTransferOffset + Constants.DefaultBlockSize,
                                    this.totalLength);

                                canUpload = true;
                            }
                        }
                    }
                }
                else
                {
                    // Source blob is a page blob
                    if (this.hasNextPage)
                    {
                        startOffset = this.nextPageToCopy.Current;

                        lock (checkpoint.TransferWindowLock)
                        {
                            if (checkpoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                            {
                                checkpoint.TransferWindow.Add(startOffset);
                                checkpoint.EntryTransferOffset = Math.Min(
                                    checkpoint.EntryTransferOffset + Constants.DefaultBlockSize,
                                    this.totalLength);

                                canUpload = true;
                            }
                        }

                        if (canUpload)
                        {
                            this.hasNextPage = this.nextPageToCopy.MoveNext();
                        }
                    }
                }

                if (!canUpload)
                {
                    return;
                }
            }

            hasWork = ((null != this.lastTransferWindow) 
                || (null != this.pagesToCopy ? 
                        this.hasNextPage : 
                        (this.TransferJob.CheckPoint.EntryTransferOffset < this.totalLength)));

            await Task.Yield();

            Uri sourceUri = this.sourceBlob.GenerateCopySourceUri();
            long length = Math.Min(this.totalLength - startOffset, Constants.DefaultBlockSize);

            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                this.destLocation.AccessCondition,
                this.destLocation.CheckedAccessCondition);

            await this.destBlob.WritePagesAsync(
                sourceUri,
                startOffset,
                length,
                startOffset,
                null,
                null,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            this.destLocation.CheckedAccessCondition = true;

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

            await this.destBlob.SetPropertiesAsync(
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken);

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
    }
}
