//------------------------------------------------------------------------------
// <copyright file="RangeBasedServiceSideSyncCopy.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    internal abstract class RangeBasedServiceSideSyncCopy : ServiceSideSyncCopyController
    {
        CountdownEvent countdownEvent;

        private Queue<long> lastTransferWindow;

        private long nextRangesSpanOffset = 0;
        private CountdownEvent getRangesCountdownEvent = null;

        private ConcurrentBag<List<long>> pageListBag = null;
        private List<long> pagesToCopy = null;
        private IEnumerator<long> nextPageToCopy = null;
        private bool hasNextPage = false;
        private ServiceSideSyncCopySource.IRangeBasedSourceHandler rangeBasedSourceHandler = null;

        protected RangeBasedServiceSideSyncCopy(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
        }

        override protected void PostFetchSourceAttributes()
        {
            if (this.SourceHandler is ServiceSideSyncCopySource.IRangeBasedSourceHandler)
            {
                this.rangeBasedSourceHandler = this.SourceHandler as ServiceSideSyncCopySource.IRangeBasedSourceHandler;
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

        protected override async Task DoPreCopyAsync()
        {
            this.hasWork = false;
            long rangeSpanOffset = this.nextRangesSpanOffset;
            long rangeSpanLength = Math.Min(Constants.PageRangesSpanSize, this.SourceHandler.TotalLength - rangeSpanOffset);

            this.nextRangesSpanOffset += Constants.PageRangesSpanSize;
            this.hasWork = (this.nextRangesSpanOffset < this.SourceHandler.TotalLength);

            var pageRanges = await this.rangeBasedSourceHandler.GetCopyRangesAsync(rangeSpanOffset, rangeSpanLength, this.CancellationToken).ConfigureAwait(false);

            long pageOffset = rangeSpanOffset;
            List<long> pageList = new List<long>();
            foreach (var pageRange in pageRanges)
            {
                if (pageOffset <= pageRange.StartOffset)
                {
                    while (pageOffset + Constants.DefaultTransferChunkSize < pageRange.StartOffset)
                    {
                        pageOffset += Constants.DefaultTransferChunkSize;
                    }

                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultTransferChunkSize;
                }

                // pageOffset > pageRange.StartOffset
                while (pageOffset < pageRange.EndOffset)
                {
                    pageList.Add(pageOffset);
                    pageOffset += Constants.DefaultTransferChunkSize;
                }
            }

            this.pageListBag.Add(pageList);

            if (this.getRangesCountdownEvent.Signal())
            {
                this.pagesToCopy = new List<long>();
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
            if (null != this.rangeBasedSourceHandler)
            {
                this.nextRangesSpanOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
                if (this.nextRangesSpanOffset == this.SourceHandler.TotalLength)
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
                else if ((this.SourceHandler.TotalLength - this.nextRangesSpanOffset) <= TransferManager.Configurations.BlockSize)
                {
                    this.InitializeCopyStatus();
                    this.state = State.Copy;
                }
                else
                {
                    int rangeSpanCount = (int)Math.Ceiling(((double)(this.SourceHandler.TotalLength - this.nextRangesSpanOffset)) / Constants.PageRangesSpanSize);
                    this.getRangesCountdownEvent = new CountdownEvent(rangeSpanCount);
                    this.pageListBag = new ConcurrentBag<List<long>>();
                    this.state = State.PreCopy;
                }
            }
            else
            {
                if (0 == this.InitializeCopyStatus())
                {
                    this.state = State.Commit;
                }
                else
                {
                    this.state = State.Copy;
                }   
            }
        }

        private int InitializeCopyStatus()
        {
            int pageLength = TransferManager.Configurations.BlockSize;

            SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;

            if ((null != checkpoint.TransferWindow)
                && (0 != checkpoint.TransferWindow.Count))
            {
                this.lastTransferWindow = new Queue<long>(checkpoint.TransferWindow);
            }

            int blockCount = (null == this.lastTransferWindow ? 0 : this.lastTransferWindow.Count)
                + (int)Math.Ceiling((double)(this.SourceHandler.TotalLength - checkpoint.EntryTransferOffset) / pageLength);
            this.countdownEvent = new CountdownEvent(blockCount);

            return blockCount;
        }

        override protected async Task GetDestinationAsync()
        {
            this.hasWork = false;

            await this.CheckAndCreateDestinationAsync().ConfigureAwait(false);

            PrepareForCopy();
            this.hasWork = true;
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

                            if (checkpoint.EntryTransferOffset < this.SourceHandler.TotalLength)
                            {
                                checkpoint.TransferWindow.Add(startOffset);
                                checkpoint.EntryTransferOffset = Math.Min(
                                    checkpoint.EntryTransferOffset + Constants.DefaultTransferChunkSize,
                                    this.SourceHandler.TotalLength);

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
                                    checkpoint.EntryTransferOffset + Constants.DefaultTransferChunkSize,
                                    this.SourceHandler.TotalLength);

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
                        (this.TransferJob.CheckPoint.EntryTransferOffset < this.SourceHandler.TotalLength)));

            await Task.Yield();

            Uri sourceUri = this.SourceHandler.GetCopySourceUri();
            long length = Math.Min(this.SourceHandler.TotalLength - startOffset, Constants.DefaultTransferChunkSize);

            await this.CopyChunkFromUriAsync(startOffset, length).ConfigureAwait(false);

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

            await this.CommonCommitAsync().ConfigureAwait(false);

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

        protected abstract Task CopyChunkFromUriAsync(long startOffset, long length);
    }
}
