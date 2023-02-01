//------------------------------------------------------------------------------
// <copyright file="RangeBasedReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    internal abstract class RangeBasedReader : TransferReaderWriterBase
    {
        /// <summary>
        /// Minimum size of empty range, the empty ranges which is smaller than this size will be merged to the adjacent range with data.
        /// </summary>
        const int MinimumNoDataRangeSize = 8 * 1024;

        private volatile State state;
        private TransferJob transferJob;
        private CountdownEvent getRangesCountDownEvent;
        private CountdownEvent toDownloadItemsCountdownEvent;
        private int getRangesSpanIndex = 0;
        private List<Utils.RangesSpan> rangesSpanList;
        private List<Utils.Range> rangeList;
        private int nextDownloadIndex = 0;
        private long lastTransferOffset;
        private TransferDownloadBuffer currentDownloadBuffer = null;

        private volatile bool hasWork;

        public RangeBasedReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.transferJob = this.SharedTransferData.TransferJob;
            this.Location = this.transferJob.Source;
            this.hasWork = true;
        }

        private enum State
        {
            FetchAttributes,
            GetRanges,
            Download,
            Error,
            Finished
        };

        public override async Task DoWorkInternalAsync()
        {
            try
            {
                switch (this.state)
                {
                    case State.FetchAttributes:
                        await this.FetchAttributesAsync();
                        break;
                    case State.GetRanges:
                        await this.GetRangesAsync();
                        break;
                    case State.Download:
                        await this.DownloadRangeAsync();
                        break;
                    default:
                        break;
                }
            }
            catch
            {
                this.state = State.Error;
                throw;
            }
        }

        public override bool HasWork
        {
            get 
            {
                return this.hasWork;
            }
        }

        public override bool IsFinished
        {
            get
            {
                return State.Error == this.state || State.Finished == this.state;
            }
        }

        protected TransferLocation Location
        {
            get;
            private set;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                if (null != this.getRangesCountDownEvent)
                {
                    this.getRangesCountDownEvent.Dispose();
                    this.getRangesCountDownEvent = null;
                }

                if (null != this.toDownloadItemsCountdownEvent)
                {
                    this.toDownloadItemsCountdownEvent.Dispose();
                    this.toDownloadItemsCountdownEvent = null;
                }
            }
        }

        private async Task FetchAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchAttributes,
                "FetchAttributesAsync called, but state isn't FetchAttributes");

            this.hasWork = false;
            this.NotifyStarting();

            try
            {
                await Utils.ExecuteXsclApiCallAsync(
                    async () => await this.DoFetchAttributesAsync(),
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
                // Getting a storage exception is expected if the blob doesn't
                // exist. For those cases that indicate the blob doesn't exist
                // we will set a specific error state.
                if (null != e.RequestInformation &&
                    e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    throw new InvalidOperationException(Resources.SourceBlobDoesNotExistException, e);
                }
                else
                {
                    throw;
                }
            }

            if (this.Location.Type == TransferLocationType.AzureBlob)
            {
                (this.Location as AzureBlobLocation).CheckedAccessCondition = true;
            }
            else if (this.Location.Type == TransferLocationType.AzureFile)
            {
                (this.Location as AzureFileLocation).CheckedAccessCondition = true;
            }

            this.Controller.CheckCancellation();

            this.state = State.GetRanges;
            this.PrepareToGetRanges();

            if (!this.rangesSpanList.Any())
            {
                // InitDownloadInfo will set hasWork.
                this.InitDownloadInfo();
                this.PreProcessed = true;
                return;
            }

            this.PreProcessed = true;
            this.hasWork = true;
        }

        private async Task GetRangesAsync()
        {
            Debug.Assert(
                (this.state == State.GetRanges) || (this.state == State.Error),
                "GetRangesAsync called, but state isn't GetRanges or Error");

            this.hasWork = false;

            this.lastTransferOffset = this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset;

            int spanIndex = Interlocked.Increment(ref this.getRangesSpanIndex);

            this.hasWork = spanIndex < (this.rangesSpanList.Count - 1);

            Utils.RangesSpan rangesSpan = this.rangesSpanList[spanIndex];

            rangesSpan.Ranges = await this.DoGetRangesAsync(rangesSpan);

            List<Utils.Range> ranges = new List<Utils.Range>();
            Utils.Range currentRange = null;
            long currentStartOffset = rangesSpan.StartOffset;

            foreach (var range in rangesSpan.Ranges)
            { 
                long emptySize = range.StartOffset - currentStartOffset;
                if (emptySize > 0 && emptySize < MinimumNoDataRangeSize)
                {
                    // There is empty range which size is smaller than MinimumNoDataRangeSize
                    // merge it to the adjacent data range.
                    if (null == currentRange)
                    {
                        currentRange = new Utils.Range()
                        {
                            StartOffset = currentStartOffset,
                            EndOffset = range.EndOffset,
                            HasData = range.HasData
                        };
                    }
                    else
                    {
                        currentRange.EndOffset = range.EndOffset;
                    }
                }
                else
                {
                    // Empty range size is larger than MinimumNoDataRangeSize
                    // put current data range in list and start to deal with the next data range.
                    if (null != currentRange)
                    {
                        ranges.Add(currentRange);
                    }

                    currentRange = new Utils.Range
                    {
                        StartOffset = range.StartOffset,
                        EndOffset = range.EndOffset,
                        HasData = range.HasData
                    };
                }

                currentStartOffset = range.EndOffset + 1;
            }

            if (null != currentRange)
            {
                ranges.Add(currentRange);
            }

            rangesSpan.Ranges = ranges;

            if (this.getRangesCountDownEvent.Signal())
            {
                this.ArrangeRanges();

                // Don't call CallFinish here, InitDownloadInfo will call it.
                this.InitDownloadInfo();
            }
        }

        private async Task DownloadRangeAsync()
        {
            Debug.Assert(
                this.state == State.Error || this.state == State.Download,
                "DownloadRangeAsync called, but state isn't Download or Error");

            this.hasWork = false;

            if (State.Error == this.state)
            {
                // Some thread has set error message, just return here.
                return;
            }

            if (this.nextDownloadIndex < this.rangeList.Count)
            {
                Utils.Range rangeData = this.rangeList[this.nextDownloadIndex];

                int blockSize = this.SharedTransferData.BlockSize;
                long blockStartOffset = (rangeData.StartOffset / blockSize) * blockSize;
                long nextBlockStartOffset = Math.Min(blockStartOffset + blockSize, this.SharedTransferData.TotalLength);

                TransferDownloadStream downloadStream = null;

                if ((rangeData.StartOffset > blockStartOffset) && (rangeData.EndOffset < nextBlockStartOffset))
                {
                    Debug.Assert(null != this.currentDownloadBuffer, "Download buffer should have been allocated when range start offset is not block size aligned");
                    downloadStream = new TransferDownloadStream(this.Scheduler.MemoryManager, this.currentDownloadBuffer, (int)(rangeData.StartOffset - blockStartOffset), (int)(rangeData.EndOffset + 1 - rangeData.StartOffset));
                }
                else
                {
                    // Attempt to reserve memory. If none available we'll
                    // retry some time later.
                    byte[][] memoryBuffer = this.Scheduler.MemoryManager.RequireBuffers(this.SharedTransferData.MemoryChunksRequiredEachTime);

                    if (null == memoryBuffer)
                    {
                        this.SetRangeDownloadHasWork();
                        return;
                    }

                    if (rangeData.EndOffset >= this.lastTransferOffset)
                    {
                        bool canRead = true;
                        lock (this.transferJob.CheckPoint.TransferWindowLock)
                        {
                            if (this.transferJob.CheckPoint.TransferWindow.Count >= Constants.MaxCountInTransferWindow)
                            {
                                canRead = false;
                            }
                            else
                            {
                                if (this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
                                {
                                    this.transferJob.CheckPoint.TransferWindow.Add(this.transferJob.CheckPoint.EntryTransferOffset);
                                    this.transferJob.CheckPoint.EntryTransferOffset = Math.Min(this.transferJob.CheckPoint.EntryTransferOffset + blockSize, this.SharedTransferData.TotalLength);
                                }
                            }
                        }

                        if (!canRead)
                        {
                            this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                            this.SetRangeDownloadHasWork();
                            return;
                        }
                    }

                    if (rangeData.StartOffset == blockStartOffset)
                    {
                        this.currentDownloadBuffer = new TransferDownloadBuffer(blockStartOffset, (int)Math.Min(blockSize, this.SharedTransferData.TotalLength - blockStartOffset), memoryBuffer);
                        downloadStream = new TransferDownloadStream(this.Scheduler.MemoryManager, this.currentDownloadBuffer, 0, (int)(rangeData.EndOffset + 1 - rangeData.StartOffset));
                    }
                    else
                    {
                        Debug.Assert(null != this.currentDownloadBuffer, "Download buffer should have been allocated when range start offset is not block size aligned");

                        TransferDownloadBuffer nextBuffer = new TransferDownloadBuffer(nextBlockStartOffset, (int)Math.Min(blockSize, this.SharedTransferData.TotalLength - nextBlockStartOffset), memoryBuffer);
                        
                        downloadStream = new TransferDownloadStream(
                            this.Scheduler.MemoryManager, 
                            this.currentDownloadBuffer, 
                            (int)(rangeData.StartOffset - blockStartOffset), 
                            (int)(nextBlockStartOffset - rangeData.StartOffset),
                            nextBuffer, 
                            0, 
                            (int)(rangeData.EndOffset + 1 - nextBlockStartOffset));

                        this.currentDownloadBuffer = nextBuffer;
                    }
                }

                using (downloadStream)
                {
                    this.nextDownloadIndex++;
                    this.SetRangeDownloadHasWork();

                    RangeBasedDownloadState rangeBasedDownloadState = new RangeBasedDownloadState
                    {
                        Range = rangeData,
                        DownloadStream = downloadStream
                    };

                    await this.DownloadRangeAsync(rangeBasedDownloadState);
                }

                this.SetChunkFinish();
                return;
            }

            this.SetRangeDownloadHasWork();
        }

        private void SetRangeDownloadHasWork()
        {
            if (this.HasWork)
            {
                return;
            }

            // Check if we have ranges available to download.
            if (this.nextDownloadIndex < this.rangeList.Count)
            {
                this.hasWork = true;
                return;
            }
        }

        private async Task DownloadRangeAsync(RangeBasedDownloadState asyncState)
        {
            Debug.Assert(null != asyncState, "asyncState object expected");
            Debug.Assert(
                this.state == State.Download || this.state == State.Error,
                "DownloadRangeAsync called, but state isn't Download or Error");

            // If a parallel operation caused the controller to be placed in
            // error state exit early to avoid unnecessary I/O.
            if (this.state == State.Error)
            {
                return;
            }

            if (asyncState.Range.HasData)
            {
                await Utils.ExecuteXsclApiCallAsync(
                    async () => await this.DoDownloadRangeToStreamAsync(asyncState),
                    this.CancellationToken);
            }
            else
            {
                // Zero memory buffer.
                asyncState.DownloadStream.SetAllZero();
            }

            asyncState.DownloadStream.FinishWrite();
            asyncState.DownloadStream.ReserveBuffer = true;

            foreach (var buffer in asyncState.DownloadStream.GetBuffers())
            {
                // Two download streams can refer to the same download buffer instance. It may cause the download
                // buffer be added into shared transfer data twice if only buffer.Finished is checked here:
                //   Thread A: FinishedWrite()
                //   Thread B: FinishedWrite(), buffer.Finished is true now
                //   Thread A: Check buffer.Finished
                //   Thread B: Check buffer.Finished
                //   Thread A: Add buffer into sharedTransferData
                //   Thread C: Writer remove buffer from sharedTransferData
                //   Thread B: Add buffer into sharedTransferData again
                // So call MarkAsProcessed to make sure buffer is added exactly once.
                if (buffer.Finished && buffer.MarkAsProcessed())
                {
                    TransferData transferData = new TransferData(this.Scheduler.MemoryManager)
                    {
                        StartOffset = buffer.StartOffset,
                        Length = buffer.Length,
                        MemoryBuffer = buffer.MemoryBuffer
                    };

                    this.SharedTransferData.AvailableData.TryAdd(buffer.StartOffset, transferData);
                }
            }
        }

        /// <summary>
        /// It might fail to get large ranges list from storage. This method is to split the whole file to spans of 148MB to get ranges.
        /// In restartable, we only need to get ranges for chunks in TransferWindow and after TransferEntryOffset in check point.
        /// In TransferWindow, there might be some chunks adjacent to TransferEntryOffset, so this method will first merge these chunks into TransferEntryOffset;
        /// Then in remained chunks in the TransferWindow, it's very possible that ranges of several chunks can be got in one 148MB span. 
        /// To avoid sending too many get ranges requests, this method will merge the chunks to 148MB spans.
        /// </summary>
        private void PrepareToGetRanges()
        {
            this.getRangesSpanIndex = -1;
            this.rangesSpanList = new List<Utils.RangesSpan>();
            this.rangeList = new List<Utils.Range>();

            this.nextDownloadIndex = 0;

            SingleObjectCheckpoint checkpoint = this.transferJob.CheckPoint;
            int blockSize = this.SharedTransferData.BlockSize;

            Utils.RangesSpan rangesSpan = null;

            if ((null != checkpoint.TransferWindow)
                && (checkpoint.TransferWindow.Any()))
            {
                checkpoint.TransferWindow.Sort();

                long lastOffset = 0;
                if (checkpoint.EntryTransferOffset == this.SharedTransferData.TotalLength)
                {
                    long lengthBeforeLastChunk = checkpoint.EntryTransferOffset % blockSize;
                    lastOffset = 0 == lengthBeforeLastChunk ? 
                        checkpoint.EntryTransferOffset - blockSize : 
                        checkpoint.EntryTransferOffset - lengthBeforeLastChunk;
                }
                else
                {
                    lastOffset = checkpoint.EntryTransferOffset - blockSize;
                }

                for (int i = checkpoint.TransferWindow.Count - 1; i >= 0; i--)
                {
                    if (lastOffset == checkpoint.TransferWindow[i])
                    {
                        checkpoint.TransferWindow.RemoveAt(i);
                        checkpoint.EntryTransferOffset = lastOffset;
                    }
                    else if (lastOffset < checkpoint.TransferWindow[i])
                    {
                        throw new FormatException(Resources.RestartableInfoCorruptedException);
                    }
                    else
                    {
                        break;
                    }

                    lastOffset = checkpoint.EntryTransferOffset - blockSize;
                }

                if (this.transferJob.CheckPoint.TransferWindow.Any())
                {
                    rangesSpan = new Utils.RangesSpan();
                    rangesSpan.StartOffset = checkpoint.TransferWindow[0];
                    rangesSpan.EndOffset = Math.Min(rangesSpan.StartOffset + Constants.PageRangesSpanSize, this.SharedTransferData.TotalLength) - 1;

                    for (int i = 1; i < checkpoint.TransferWindow.Count; ++i )
                    {
                        if (checkpoint.TransferWindow[i] + blockSize > rangesSpan.EndOffset)
                        {
                            long lastEndOffset = rangesSpan.EndOffset;
                            this.rangesSpanList.Add(rangesSpan);
                            rangesSpan = new Utils.RangesSpan();
                            rangesSpan.StartOffset = checkpoint.TransferWindow[i] > lastEndOffset ? checkpoint.TransferWindow[i] : lastEndOffset + 1;
                            rangesSpan.EndOffset = Math.Min(rangesSpan.StartOffset + Constants.PageRangesSpanSize, this.SharedTransferData.TotalLength) - 1;
                        }
                    }

                    this.rangesSpanList.Add(rangesSpan);
                }
            }

            long offset = null != rangesSpan ?
                rangesSpan.EndOffset > checkpoint.EntryTransferOffset ?
                rangesSpan.EndOffset + 1 : 
                checkpoint.EntryTransferOffset : 
                checkpoint.EntryTransferOffset;

            while (offset < this.SharedTransferData.TotalLength)
            {
                rangesSpan = new Utils.RangesSpan()
                {
                    StartOffset = offset,
                    EndOffset = Math.Min(offset + Constants.PageRangesSpanSize, this.SharedTransferData.TotalLength) - 1
                };

                this.rangesSpanList.Add(rangesSpan);
                offset = rangesSpan.EndOffset + 1;
            }

            if (!this.rangesSpanList.Any())
            {
                return;
            }
            else if (this.rangesSpanList.Count == 1)
            {
                if (this.rangesSpanList[0].EndOffset - this.rangesSpanList[0].StartOffset < blockSize)
                {
                    this.rangeList.Add(new Utils.Range()
                    {
                        StartOffset = this.rangesSpanList[0].StartOffset,
                        EndOffset = this.rangesSpanList[0].EndOffset,
                        HasData = true
                    });

                    this.rangesSpanList.Clear();
                    return;
                }
            }

            this.getRangesCountDownEvent = new CountdownEvent(this.rangesSpanList.Count);
        }

        private void ClearForGetRanges()
        {
            this.rangesSpanList = null;

            if (null != this.getRangesCountDownEvent)
            {
                this.getRangesCountDownEvent.Dispose();
                this.getRangesCountDownEvent = null;
            }
        }

        /// <summary>
        /// Turn raw ranges get from Azure Storage in rangesSpanList
        /// into list of Range.
        /// </summary>
        private void ArrangeRanges()
        {
            long currentEndOffset = -1;

            // 1st RangesSpan (148MB)
            IEnumerator<Utils.RangesSpan> enumerator = this.rangesSpanList.GetEnumerator();
            bool hasValue = enumerator.MoveNext();
            bool reachLastTransferOffset = false;
            int lastTransferWindowIndex = 0;

            Utils.RangesSpan current;
            Utils.RangesSpan next;

            if (hasValue)
            {
                // 1st 148MB
                current = enumerator.Current;

                while (hasValue)
                {
                    hasValue = enumerator.MoveNext();

                    // 1st 148MB doesn't have any data
                    if (!current.Ranges.Any())
                    {
                        // 2nd 148MB
                        current = enumerator.Current;
                        continue;
                    }

                    if (hasValue)
                    {
                        // 2nd 148MB
                        next = enumerator.Current;
                        
                        Debug.Assert(
                            current.EndOffset < this.transferJob.CheckPoint.EntryTransferOffset
                            || ((current.EndOffset + 1) == next.StartOffset),
                            "Something wrong with ranges list.");

                        // Both 1st 148MB & 2nd 148MB has data
                        if (next.Ranges.Any())
                        {
                            // They are connected, merge the range
                            if ((current.Ranges.Last().EndOffset + 1) == next.Ranges.First().StartOffset)
                            {
                                Utils.Range mergedRange = new Utils.Range()
                                {
                                    StartOffset = current.Ranges.Last().StartOffset,
                                    EndOffset = next.Ranges.First().EndOffset,
                                    HasData = true
                                };

                                // Remove the last range in 1st 148MB and first range in 2nd 148MB
                                current.Ranges.RemoveAt(current.Ranges.Count - 1);
                                next.Ranges.RemoveAt(0);

                                // Add the merged range to 1st *148MB* (not 148MB anymore)
                                current.Ranges.Add(mergedRange);
                                current.EndOffset = mergedRange.EndOffset;
                                next.StartOffset = mergedRange.EndOffset + 1;

                                if (next.EndOffset == mergedRange.EndOffset)
                                {
                                    continue;
                                }
                            }
                        }
                    }

                    foreach (Utils.Range range in current.Ranges)
                    {
                        // Check if we have a gap before the current range.
                        // If so we'll generate a range with HasData = false.
                        if (currentEndOffset != range.StartOffset - 1)
                        {
                            // Add empty ranges based on gaps
                            this.AddRangesByCheckPoint(
                                currentEndOffset + 1,
                                range.StartOffset - 1,
                                false,
                                ref reachLastTransferOffset,
                                ref lastTransferWindowIndex);
                        }

                        this.AddRangesByCheckPoint(
                            range.StartOffset,
                            range.EndOffset,
                            true,
                            ref reachLastTransferOffset,
                            ref lastTransferWindowIndex);

                        currentEndOffset = range.EndOffset;
                    }

                    current = enumerator.Current;
                }
            }

            if (currentEndOffset < this.SharedTransferData.TotalLength - 1)
            {
                this.AddRangesByCheckPoint(
                    currentEndOffset + 1,
                    this.SharedTransferData.TotalLength - 1,
                    false,
                    ref reachLastTransferOffset,
                    ref lastTransferWindowIndex);
            }
        }

        private void AddRangesByCheckPoint(long startOffset, long endOffset, bool hasData, ref bool reachLastTransferOffset, ref int lastTransferWindowIndex)
        {
            SingleObjectCheckpoint checkpoint = this.transferJob.CheckPoint;
            if (reachLastTransferOffset)
            {
                this.rangeList.AddRange(
                    new Utils.Range
                    {
                        StartOffset = startOffset,
                        EndOffset = endOffset,
                        HasData = hasData,
                    }.SplitRanges(Constants.DefaultTransferChunkSize));
            }
            else
            {
                Utils.Range range = new Utils.Range()
                {
                    StartOffset = -1,
                    HasData = hasData
                };

                while (lastTransferWindowIndex < checkpoint.TransferWindow.Count)
                {
                    long lastTransferWindowStart = checkpoint.TransferWindow[lastTransferWindowIndex];
                    long lastTransferWindowEnd = Math.Min(checkpoint.TransferWindow[lastTransferWindowIndex] + this.SharedTransferData.BlockSize - 1, this.SharedTransferData.TotalLength);

                    if (lastTransferWindowStart <= endOffset)
                    {
                        if (-1 == range.StartOffset)
                        {
                            // New range
                            range.StartOffset = Math.Max(lastTransferWindowStart, startOffset);
                            range.EndOffset = Math.Min(lastTransferWindowEnd, endOffset);
                        }
                        else
                        {
                            if (range.EndOffset != lastTransferWindowStart - 1)
                            {
                                // Store the previous range and create a new one
                                this.rangeList.AddRange(range.SplitRanges(Constants.DefaultTransferChunkSize));
                                range = new Utils.Range()
                                {
                                    StartOffset = Math.Max(lastTransferWindowStart, startOffset),
                                    HasData = hasData
                                };
                            }

                            range.EndOffset = Math.Min(lastTransferWindowEnd, endOffset);
                        }

                        if (range.EndOffset == lastTransferWindowEnd)
                        {
                            // Reach the end of transfer window, move to next
                            ++lastTransferWindowIndex;
                            continue;
                        }
                    }

                    break;
                }

                if (-1 != range.StartOffset)
                {
                    this.rangeList.AddRange(range.SplitRanges(Constants.DefaultTransferChunkSize));
                }

                if (checkpoint.EntryTransferOffset <= endOffset + 1)
                {
                    reachLastTransferOffset = true;

                    if (checkpoint.EntryTransferOffset <= endOffset)
                    {
                        this.rangeList.AddRange(new Utils.Range()
                        {
                            StartOffset = checkpoint.EntryTransferOffset,
                            EndOffset = endOffset,
                            HasData = hasData,
                        }.SplitRanges(Constants.DefaultTransferChunkSize));
                    }
                }
            }
        }

        /// <summary>
        /// To initialize range based object download related information in the controller.
        /// This method will call CallFinish.
        /// </summary>
        private void InitDownloadInfo()
        {
            this.ClearForGetRanges();

            this.state = State.Download;

            if (this.rangeList.Count == this.nextDownloadIndex)
            {
                this.toDownloadItemsCountdownEvent = new CountdownEvent(1);                
                this.SetChunkFinish();
            }
            else
            {
                this.toDownloadItemsCountdownEvent = new CountdownEvent(this.rangeList.Count);
                this.hasWork = true;
            }
        }

        private void SetChunkFinish()
        {
            if (this.toDownloadItemsCountdownEvent.Signal())
            {
                this.state = State.Finished;
                this.hasWork = false;
            }
        }       

        protected class RangeBasedDownloadState
        {
            private Utils.Range range;

            public Utils.Range Range
            {
                get
                {
                    return this.range;
                }

                set
                {
                    this.range = value;

                    this.StartOffset = value.StartOffset;
                    this.Length = (int)(value.EndOffset - value.StartOffset + 1);
                }
            }

            /// <summary>
            /// Gets or sets a handle to the memory buffer to ensure the
            /// memory buffer remains in memory during the entire operation.
            /// </summary>
            public TransferDownloadStream DownloadStream
            {
                get;
                set;
            }

            /// <summary>
            /// Gets or sets the starting offset of this part of data.
            /// </summary>
            public long StartOffset
            {
                get;
                set;
            }

            /// <summary>
            /// Gets or sets the length of this part of data.
            /// </summary>
            public int Length
            {
                get;
                set;
            }
        }
        
        protected abstract Task DoFetchAttributesAsync();

        protected abstract Task DoDownloadRangeToStreamAsync(RangeBasedDownloadState asyncState);

        protected abstract Task<List<Utils.Range>> DoGetRangesAsync(Utils.RangesSpan rangesSpan);
    }
}
