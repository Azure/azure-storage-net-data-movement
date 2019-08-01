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
    class PageBlobServiceSideSyncCopyController : ServiceSideSyncCopyController
    {
        private CloudPageBlob sourcePageBlob;
        private CloudPageBlob destPageBlob;
        CountdownEvent countdownEvent;

        private Queue<long> lastTransferWindow;

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
            this.destPageBlob = destLocation.Blob as CloudPageBlob;
            this.hasWork = true;
        }

        override protected void PostFetchSourceAttributes()
        {
            if (this.sourceBlob.BlobType == BlobType.PageBlob)
            {
                this.sourcePageBlob = this.sourceBlob as CloudPageBlob;
                this.pagesToCopy = new List<long>();
                this.pageListBag = new ConcurrentBag<List<long>>();
            }

            if (0 == this.TransferJob.CheckPoint.EntryTransferOffset)
            {
                this.state = State.GetDestination;
            }
            else
            {
                this.PrepareForCopy();
            }

            this.hasWork = true;
        }

        protected override async Task CreateDestinationAsync(AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            await this.destPageBlob.CreateAsync(
                this.totalLength,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                cancellationToken);
        }

        override protected async Task GetDestinationAsync()
        {
            this.hasWork = false;

            await this.CheckAndCreateDestinationAsync();

            if (0 != this.totalLength)
            {
                await this.destPageBlob.ClearPagesAsync(0, this.totalLength,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                        Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.TransferContext),
                        this.CancellationToken);
            }

            PrepareForCopy();
            this.hasWork = true;
        }

        private void PrepareForCopy()
        {
            if (null != this.sourcePageBlob)
            {
                this.nextRangesSpanOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
                if (this.nextRangesSpanOffset == this.totalLength)
                {
                    this.InitializeCopyStatus();
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
                    int rangeSpanCount = (int)Math.Ceiling(((double)(this.totalLength - this.nextRangesSpanOffset)) / Constants.PageRangesSpanSize);
                    this.getRangesCountdownEvent = new CountdownEvent(rangeSpanCount);
                    this.state = State.PreCopy;
                }
            }
            else
            {
                this.InitializeCopyStatus();
                this.state = State.Copy;
            }
        }

        protected override async Task DoPreCopyAsync()
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
                    while (pageOffset + Constants.DefaultChunkSize < pageRange.StartOffset)
                    {
                        pageOffset += Constants.DefaultChunkSize;
                    }

                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultChunkSize;
                }

                // pageOffset > pageRange.StartOffset
                while (pageOffset < pageRange.EndOffset)
                {
                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultChunkSize;
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

        private void InitializeCopyStatus()
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

        protected override async Task CopyChunkAsync()
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
                                    checkpoint.EntryTransferOffset + Constants.DefaultChunkSize,
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
                                    checkpoint.EntryTransferOffset + Constants.DefaultChunkSize,
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
            long length = Math.Min(this.totalLength - startOffset, Constants.DefaultChunkSize);

            await this.destPageBlob.WritePagesAsync(
                sourceUri,
                startOffset,
                length,
                startOffset,
                null,
                Utils.GenerateIfMatchConditionWithCustomerCondition(this.sourceLocation.ETag, this.sourceLocation.AccessCondition, true),
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
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

            await this.CommonCommitAsync();

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
