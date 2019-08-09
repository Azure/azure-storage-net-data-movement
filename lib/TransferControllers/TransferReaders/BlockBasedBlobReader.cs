//------------------------------------------------------------------------------
// <copyright file="BlockBasedBlobReader.cs" company="Microsoft">
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
    using Microsoft.Azure.Storage.Blob;

    internal sealed class BlockBasedBlobReader : TransferReaderWriterBase
    {
        /// <summary>
        /// Instance to represent source location.
        /// </summary>
        private readonly AzureBlobLocation sourceLocation;

        /// <summary>
        /// Block/append blob instance to be downloaded from.
        /// </summary>
        private readonly CloudBlob sourceBlob;

        /// <summary>
        /// Window to record unfinished chunks to be retransferred again.
        /// </summary>
        private Queue<long> lastTransferWindow;

        private readonly TransferJob transferJob;

        /// <summary>
        /// Value to indicate whether the transfer is finished. 
        /// This is to tell the caller that the reader can be disposed,
        /// Both error happened or completed will be treated to be finished.
        /// </summary>
        private volatile bool isFinished = false;

        /// <summary>
        /// Work token indicates whether this reader has work, could be 0(no work) or 1(has work).
        /// </summary>
        private volatile int workToken;

        private volatile bool isStateSwitchedInternal;

        private CountdownEvent downloadCountdownEvent;

        public BlockBasedBlobReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.transferJob = this.SharedTransferData.TransferJob;
            this.sourceLocation = this.transferJob.Source as AzureBlobLocation;
            this.sourceBlob = this.sourceLocation?.Blob;

            Debug.Assert(
                (this.sourceBlob is CloudBlockBlob) || (this.sourceBlob is CloudAppendBlob),
            "Initializing BlockBlobReader while source location is not a block blob or an append blob.");

            this.workToken = 1;
        }

        public override bool IsFinished
        {
            get
            {
                return this.isFinished;
            }
        }

        public override bool HasWork
        {
            get
            {
                return this.workToken == 1;
            }
        }

        public override async Task DoWorkInternalAsync()
        {
            try
            {
                if (!this.PreProcessed)
                {
                    await this.FetchAttributeAsync();
                }
                else
                {
                    await this.DownloadBlockBlobAsync();
                }
            }
            catch (Exception)
            {
                this.isFinished = true;
                throw;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                if (null != this.downloadCountdownEvent)
                {
                    this.downloadCountdownEvent.Dispose();
                    this.downloadCountdownEvent = null;
                }
            }
        }

        private async Task FetchAttributeAsync()
        {
            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            await Task.Yield();

            this.NotifyStarting();

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
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
                            this.CancellationToken).ConfigureAwait(false);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
                {
                    var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
                catch (StorageException e)
                {
#endif
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
            }

            // Note: If the transfer job's dispatch root is directory transfer, no need to check access condition for source location.
            // If the transfer job's dispatch root is single transfer, access condition has been checked if logic reach here.
            this.sourceLocation.CheckedAccessCondition = true;

            if (this.sourceBlob.Properties.BlobType == BlobType.Unspecified)
            {
                throw new InvalidOperationException(Resources.FailedToGetBlobTypeException);
            }

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if (0 != this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceBlob.Properties.ETag;
            }
            else if ((this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset > this.sourceBlob.Properties.Length)
                 || (this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.SharedTransferData.DisableContentMD5Validation =
                 null != this.sourceLocation.BlobRequestOptions ?
                 this.sourceLocation.BlobRequestOptions.DisableContentMD5Validation.HasValue ?
                 this.sourceLocation.BlobRequestOptions.DisableContentMD5Validation.Value : false : false;

            this.SharedTransferData.TotalLength = this.sourceBlob.Properties.Length;

            this.SharedTransferData.Attributes = Utils.GenerateAttributes(this.sourceBlob);

            if ((0 == this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                 && (null != this.SharedTransferData.TransferJob.CheckPoint.TransferWindow)
                 && (0 != this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Count))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            // Only initialize private queue when TransferWindow is necessary.
            if (!this.EnableOneChunkFileOptimization)
            {
                this.lastTransferWindow = new Queue<long>(this.SharedTransferData.TransferJob.CheckPoint.TransferWindow);
            }

            int downloadCount = (this.lastTransferWindow == null ? 0 : this.lastTransferWindow.Count) +
                 (int)Math.Ceiling((double)(this.sourceBlob.Properties.Length - this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset) / this.SharedTransferData.BlockSize);

            if (0 == downloadCount)
            {
                this.isFinished = true;
                this.PreProcessed = true;
                this.workToken = 1;

                return;
            }

            if (this.EnableOneChunkFileOptimization)
            {
                this.isStateSwitchedInternal = true;
                await this.DownloadBlockBlobAsync().ConfigureAwait(false);

                this.PreProcessed = true;
            }
            else
            {
                this.downloadCountdownEvent = new CountdownEvent(downloadCount);
                this.PreProcessed = true; // Note: PreProcessed should be set before workToken, otherwise SyncTransferController would schedule reader again before writer PreProcess
                this.workToken = 1;
            }

        }

        private async Task DownloadBlockBlobAsync()
        {
            if (!this.isStateSwitchedInternal && Interlocked.CompareExchange(ref this.workToken, 0, 1) == 0)
            {
                return;
            }

            byte[][] memoryBuffer = this.Scheduler.MemoryManager.RequireBuffers(this.SharedTransferData.MemoryChunksRequiredEachTime);

            if (null != memoryBuffer)
            {
                long startOffset = 0;

                // Only multi-chunk file need transfer window calculation.
                if (!this.EnableOneChunkFileOptimization)
                {
                    if (!this.IsTransferWindowEmpty())
                    {
                        startOffset = this.lastTransferWindow.Dequeue();
                    }
                    else
                    {
                        bool canUpload = false;

                        lock (this.transferJob.CheckPoint.TransferWindowLock)
                        {
                            if (this.transferJob.CheckPoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                            {
                                startOffset = this.transferJob.CheckPoint.EntryTransferOffset;

                                if (this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
                                {
                                    this.transferJob.CheckPoint.TransferWindow.Add(startOffset);
                                    this.transferJob.CheckPoint.EntryTransferOffset = Math.Min(
                                        this.transferJob.CheckPoint.EntryTransferOffset + this.SharedTransferData.BlockSize,
                                        this.SharedTransferData.TotalLength);

                                    canUpload = true;
                                }
                            }
                        }

                        if (!canUpload)
                        {
                            this.workToken = 1;
                            this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                            return;
                        }
                    }

                    if ((startOffset > this.SharedTransferData.TotalLength)
                        || (startOffset < 0))
                    {
                        this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                        throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                    }

                    this.SetBlockDownloadHasWork();
                }

                ReadDataState asyncState = new ReadDataState
                {
                    MemoryBuffer = memoryBuffer,
                    BytesRead = 0,
                    StartOffset = startOffset,
                    Length = (int)Math.Min(this.SharedTransferData.BlockSize, this.SharedTransferData.TotalLength - startOffset),
                    MemoryManager = this.Scheduler.MemoryManager,
                };

                using (asyncState)
                {
                    await this.DownloadChunkAsync(asyncState).ConfigureAwait(false);
                }

                return;
            }

            this.SetBlockDownloadHasWork();
        }

        private async Task DownloadChunkAsync(ReadDataState asyncState)
        {
            Debug.Assert(null != asyncState, "asyncState object expected");
            
            // Use Yield to return to scheduling main logic immediately, and to improve scheduling efficiency.
            if (!this.isStateSwitchedInternal)
            {
                await Task.Yield();
            }

            // If a parallel operation caused the controller to be placed in
            // error state exit early to avoid unnecessary I/O.
            if (this.Controller.ErrorOccurred)
            {
                return;
            }

            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                 this.sourceBlob.Properties.ETag,
                 this.sourceLocation.AccessCondition);

            if (asyncState.MemoryBuffer.Length == 1)
            {
                // We're to download this block.
                asyncState.MemoryStream =
                    new MemoryStream(
                        asyncState.MemoryBuffer[0],
                        0,
                        asyncState.Length);
                await this.sourceBlob.DownloadRangeToStreamAsync(
                         asyncState.MemoryStream,
                         asyncState.StartOffset,
                         asyncState.Length,
                         accessCondition,
                         Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                         Utils.GenerateOperationContext(this.Controller.TransferContext),
                         this.CancellationToken).ConfigureAwait(false);
            }
            else
            {
                var blockSize = Constants.DefaultTransferChunkSize; // 4MB

                var startOffset = asyncState.StartOffset;
                var remainingLength = asyncState.Length;
                var index = 0;

                do
                {
                    var length = Math.Min(blockSize, remainingLength);

                    var memoryStream = new MemoryStream(asyncState.MemoryBuffer[index], 0, length);

                    await this.sourceBlob.DownloadRangeToStreamAsync(
                            memoryStream,
                            startOffset,
                            length,
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
                            this.CancellationToken);

                    index++;
                    startOffset += length;
                    remainingLength -= length;
                } while (remainingLength > 0);
            }

            TransferData transferData = new TransferData(this.Scheduler.MemoryManager)
            {
                StartOffset = asyncState.StartOffset,
                Length = asyncState.Length,
                MemoryBuffer = asyncState.MemoryBuffer
            };

            this.SharedTransferData.AvailableData.TryAdd(transferData.StartOffset, transferData);

            // Set memory buffer to null. We don't want its dispose method to 
            // be called once our asyncState is disposed. The memory should 
            // not be reused yet, we still need to write it to disk.
            asyncState.MemoryBuffer = null;

            // When there is only one chunk, after read the only chunk, reader's work has finished, no more work to do.
            if (this.EnableOneChunkFileOptimization)
            {
                this.isFinished = true;
            }
            else
            {
                this.SetFinish();
                this.SetBlockDownloadHasWork();
            }
        }

        private void SetFinish()
        {
            if (this.downloadCountdownEvent.Signal())
            {
                this.isFinished = true;
            }
        }

        private void SetBlockDownloadHasWork()
        {
            if (this.HasWork)
            {
                return;
            }

            // Check if we have blocks available to download.
            if (!this.IsTransferWindowEmpty()
                || this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
            {
                this.workToken = 1;
                return;
            }
        }

        private bool IsTransferWindowEmpty()
        {
            return null == this.lastTransferWindow || this.lastTransferWindow.Count == 0;
        }
    }
}