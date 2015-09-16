//------------------------------------------------------------------------------
// <copyright file="BlockBasedBlobReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal sealed class BlockBasedBlobReader : TransferReaderWriterBase
    {
        /// <summary>
        /// Block/append blob instance to be downloaded from.
        /// </summary>
        private CloudBlob blob;

        /// <summary>
        /// Window to record unfinished chunks to be retransferred again.
        /// </summary>
        private Queue<long> lastTransferWindow;

        /// <summary>
        /// Instance to represent source location.
        /// </summary>
        private TransferLocation transferLocation;

        private TransferJob transferJob;

        /// <summary>
        /// Value to indicate whether the transfer is finished. 
        /// This is to tell the caller that the reader can be disposed,
        /// Both error happened or completed will be treated to be finished.
        /// </summary>
        private volatile bool isFinished = false;

        private volatile bool hasWork;

        private CountdownEvent downloadCountdownEvent;

        public BlockBasedBlobReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.transferLocation = this.SharedTransferData.TransferJob.Source;
            this.transferJob = this.SharedTransferData.TransferJob;
            this.blob = this.transferLocation.Blob;

            Debug.Assert(
                (this.blob is CloudBlockBlob) ||(this.blob is CloudAppendBlob), 
            "Initializing BlockBlobReader while source location is not a block blob or an append blob.");

            this.hasWork = true;
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
                return this.hasWork;
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
            this.hasWork = false;
            this.NotifyStarting();

            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.transferLocation.ETag,
                this.transferLocation.AccessCondition,
                this.transferLocation.CheckedAccessCondition);

            try
            {
                await this.blob.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.transferLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                    this.CancellationToken);
            }
            catch (StorageException e)
            {
                if (null != e.RequestInformation &&
                    e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    throw new InvalidOperationException(Resources.SourceBlobDoesNotExistException);
                }
                else
                {
                    throw;
                }
            }

            this.transferLocation.CheckedAccessCondition = true;

            if (this.blob.Properties.BlobType == BlobType.Unspecified)
            {
                throw new InvalidOperationException(Resources.FailedToGetBlobTypeException);
            }

            if (string.IsNullOrEmpty(this.transferLocation.ETag))
            {
                if (0 != this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.transferLocation.ETag = this.blob.Properties.ETag;
            }
            else if ((this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset > this.blob.Properties.Length)
                || (this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.SharedTransferData.SourceLocation = this.blob.Uri.ToString();

            this.SharedTransferData.DisableContentMD5Validation =
                null != this.transferLocation.BlobRequestOptions ?
                this.transferLocation.BlobRequestOptions.DisableContentMD5Validation.HasValue ?
                this.transferLocation.BlobRequestOptions.DisableContentMD5Validation.Value : false : false;

            this.SharedTransferData.TotalLength = this.blob.Properties.Length;
            this.SharedTransferData.Attributes = Utils.GenerateAttributes(this.blob);

            if ((0 == this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                && (null != this.SharedTransferData.TransferJob.CheckPoint.TransferWindow)
                && (0 != this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Count))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.lastTransferWindow = new Queue<long>(this.SharedTransferData.TransferJob.CheckPoint.TransferWindow);
            
            int downloadCount = this.lastTransferWindow.Count +
                (int)Math.Ceiling((double)(this.blob.Properties.Length - this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset) / this.Scheduler.TransferOptions.BlockSize);

            if (0 == downloadCount)
            {
                this.isFinished = true;
                this.PreProcessed = true;
                this.hasWork = true;
            }
            else
            {
                this.downloadCountdownEvent = new CountdownEvent(downloadCount);

                this.PreProcessed = true;
                this.hasWork = true;
            }
        }

        private async Task DownloadBlockBlobAsync()
        {
            this.hasWork = false;

            byte[] memoryBuffer = this.Scheduler.MemoryManager.RequireBuffer();

            if (null != memoryBuffer)
            {
                long startOffset = 0;

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
                                    this.transferJob.CheckPoint.EntryTransferOffset + this.Scheduler.TransferOptions.BlockSize,
                                    this.SharedTransferData.TotalLength);

                                canUpload = true;
                            }
                        }
                    }

                    if (!canUpload)
                    {
                        this.hasWork = true;
                        this.Scheduler.MemoryManager.ReleaseBuffer(memoryBuffer);
                        return;
                    }
                }

                if ((startOffset > this.SharedTransferData.TotalLength)
                    || (startOffset < 0))
                {
                    this.Scheduler.MemoryManager.ReleaseBuffer(memoryBuffer);
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.SetBlockDownloadHasWork();

                ReadDataState asyncState = new ReadDataState
                {
                    MemoryBuffer = memoryBuffer,
                    BytesRead = 0,
                    StartOffset = startOffset,
                    Length = (int)Math.Min(this.Scheduler.TransferOptions.BlockSize, this.SharedTransferData.TotalLength - startOffset),
                    MemoryManager = this.Scheduler.MemoryManager,
                };

                using (asyncState)
                {
                    await this.DownloadChunkAsync(asyncState);
                }

                return;
            }

            this.SetBlockDownloadHasWork();
        }

        private async Task DownloadChunkAsync(ReadDataState asyncState)
        {
            Debug.Assert(null != asyncState, "asyncState object expected");

            // If a parallel operation caused the controller to be placed in
            // error state exit early to avoid unnecessary I/O.
            if (this.Controller.ErrorOccurred)
            {
                return;
            }

            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                 this.blob.Properties.ETag,
                 this.transferLocation.AccessCondition);

            // We're to download this block.
            asyncState.MemoryStream =
                new MemoryStream(
                    asyncState.MemoryBuffer,
                    0,
                    asyncState.Length);

            await this.blob.DownloadRangeToStreamAsync(
                        asyncState.MemoryStream,
                        asyncState.StartOffset,
                        asyncState.Length,
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.transferLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.Controller.TransferContext),
                        this.CancellationToken);

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

            this.SetFinish();
            this.SetBlockDownloadHasWork();
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
                this.hasWork = true;
                return;
            }
        }

        private bool IsTransferWindowEmpty()
        {
            return null == this.lastTransferWindow || this.lastTransferWindow.Count == 0;
        }
    }
}
