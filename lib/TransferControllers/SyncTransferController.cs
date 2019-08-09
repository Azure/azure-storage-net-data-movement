//------------------------------------------------------------------------------
// <copyright file="SyncTransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal class SyncTransferController : TransferControllerBase
    {
        private readonly TransferReaderWriterBase reader;
        private readonly TransferReaderWriterBase writer;

        public SyncTransferController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
            if (null == transferScheduler)
            {
                throw new ArgumentNullException(nameof(transferScheduler));
            }

            if (null == transferJob)
            {
                throw new ArgumentNullException(nameof(transferJob));
            }

            this.SharedTransferData = new SharedTransferData()
            {
                TransferJob = this.TransferJob,
                AvailableData = new ConcurrentDictionary<long, TransferData>(),
            };

            if (null == transferJob.CheckPoint)
            {
                transferJob.CheckPoint = new SingleObjectCheckpoint();
            }

            this.reader = this.GetReader(transferJob.Source);
            this.writer = this.GetWriter(transferJob.Destination);

            this.reader.EnableSmallFileOptimization = (this.reader.EnableSmallFileOptimization && this.writer.EnableSmallFileOptimization);
            this.writer.EnableSmallFileOptimization = (this.reader.EnableSmallFileOptimization && this.writer.EnableSmallFileOptimization);

            this.SharedTransferData.OnTotalLengthChanged += (sender, args) =>
            {
                // For large block blob uploading, we need to re-calculate the BlockSize according to the total size
                // The formula: Ceiling(TotalSize / (50000 * DefaultBlockSize)) * DefaultBlockSize. This will make sure the 
                // new block size will be mutiple of DefaultBlockSize(aka MemoryManager's chunk size)
                if (this.writer is BlockBlobWriter)
                {
                    var normalMaxBlockBlobSize = (long)50000 * Constants.DefaultTransferChunkSize;

                    // Calculate the min block size according to the blob total length
                    var memoryChunksRequiredEachTime = (int)Math.Ceiling((double)this.SharedTransferData.TotalLength / normalMaxBlockBlobSize);
                    var blockSize = memoryChunksRequiredEachTime * Constants.DefaultTransferChunkSize;
                    blockSize = Math.Max(blockSize, Constants.DefaultBlockBlobBlockSize);

                    // Take the block size user specified when it's greater than the calculated value
                    if (TransferManager.Configurations.BlockSize > blockSize)
                    {
                        blockSize = TransferManager.Configurations.BlockSize;
                        memoryChunksRequiredEachTime = (int)Math.Ceiling((double)blockSize / Constants.DefaultMemoryChunkSize);
                    }
                    else
                    {
                        memoryChunksRequiredEachTime = (int)Math.Ceiling((double)blockSize / Constants.DefaultTransferChunkSize);

                        // Try to increase the memory pool size
                        this.Scheduler.TransferOptions.UpdateMaximumCacheSize(blockSize);
                    }

                    // If data size is smaller than block size, fit block size according to total length, in order to minimize buffer allocation,
                    // and save space and time.
                    if (this.SharedTransferData.TotalLength < blockSize)
                    {
                        // Note total length could be 0, in this case, use default block size.
                        memoryChunksRequiredEachTime = Math.Max(1,
                            (int)Math.Ceiling((double)this.SharedTransferData.TotalLength / Constants.DefaultMemoryChunkSize));
                        blockSize = memoryChunksRequiredEachTime * Constants.DefaultMemoryChunkSize;
                    }
                    this.SharedTransferData.BlockSize = blockSize;
                    this.SharedTransferData.MemoryChunksRequiredEachTime = memoryChunksRequiredEachTime;
                }
                else
                {
                    // For normal directions, we'll use default block size 4MB for transfer.
                    this.SharedTransferData.BlockSize = Constants.DefaultTransferChunkSize;
                    this.SharedTransferData.MemoryChunksRequiredEachTime = 1;
                }
            };
        }

        public SharedTransferData SharedTransferData
        {
            get;
            private set;
        }

        public bool ErrorOccurred
        {
            get;
            private set;
        }

        public bool hasWork = true;

        public override bool HasWork
        {
            get
            {
                if (!this.hasWork) return false;

                var hasWorkInternal = (!this.reader.PreProcessed && this.reader.HasWork)
                    || (this.reader.PreProcessed && this.writer.HasWork)
                    || (this.writer.PreProcessed && this.reader.HasWork);

                return !this.ErrorOccurred && hasWorkInternal;
            }
        }

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

            if (!this.reader.PreProcessed && this.reader.HasWork)
            {
                await this.reader.DoWorkInternalAsync();
            }
            else if (this.reader.PreProcessed && this.writer.HasWork)
            {
                await this.writer.DoWorkInternalAsync();
            }
            else if (this.writer.PreProcessed && this.reader.HasWork)
            {
                await this.reader.DoWorkInternalAsync();
            }

            return this.ErrorOccurred || this.writer.IsFinished;
        }

        protected override void SetErrorState(Exception ex)
        {
            this.ErrorOccurred = true;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private TransferReaderWriterBase GetReader(TransferLocation sourceLocation)
        {
            switch (sourceLocation.Type)
            {
                case TransferLocationType.Stream:
                    var streamedReader = new StreamedReader(this.Scheduler, this, this.CancellationToken);
                    streamedReader.EnableSmallFileOptimization = true;
                    return streamedReader;
                case TransferLocationType.FilePath:
                    var fileReader = new StreamedReader(this.Scheduler, this, this.CancellationToken);
                    fileReader.EnableSmallFileOptimization = true;
                    return fileReader;
                case TransferLocationType.AzureBlob:
                    CloudBlob sourceBlob = (sourceLocation as AzureBlobLocation).Blob;
                    if (sourceBlob is CloudPageBlob)
                    {
                        return new PageBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (sourceBlob is CloudBlockBlob)
                    {
                        var blobReader = new BlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                        blobReader.EnableSmallFileOptimization = true;
                        return blobReader;
                    }
                    else if (sourceBlob is CloudAppendBlob)
                    {
                        return new BlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.UnsupportedBlobTypeException,
                            sourceBlob.BlobType));
                    }
                case TransferLocationType.AzureFile:
                    return new CloudFileReader(this.Scheduler, this, this.CancellationToken);
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.UnsupportedTransferLocationException,
                        sourceLocation.Type));
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private TransferReaderWriterBase GetWriter(TransferLocation destLocation)
        {
            switch (destLocation.Type)
            {
                case TransferLocationType.Stream:
                    var streamWriter = new StreamedWriter(this.Scheduler, this, this.CancellationToken);
                    streamWriter.EnableSmallFileOptimization = true;
                    return streamWriter;
                case TransferLocationType.FilePath:
                    var fileWriter = new StreamedWriter(this.Scheduler, this, this.CancellationToken);
                    fileWriter.EnableSmallFileOptimization = true;
                    return fileWriter;
                case TransferLocationType.AzureBlob:
                    CloudBlob destBlob = (destLocation as AzureBlobLocation).Blob;
                    if (destBlob is CloudPageBlob)
                    {
                        return new PageBlobWriter(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (destBlob is CloudBlockBlob)
                    {
                        var blobWriter = new BlockBlobWriter(this.Scheduler, this, this.CancellationToken);
                        blobWriter.EnableSmallFileOptimization = true;
                        return blobWriter;
                    }
                    else if (destBlob is CloudAppendBlob)
                    {
                        return new AppendBlobWriter(this.Scheduler, this, this.CancellationToken);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.UnsupportedBlobTypeException,
                            destBlob.BlobType));
                    }
                case TransferLocationType.AzureFile:
                    return new CloudFileWriter(this.Scheduler, this, this.CancellationToken);
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.UnsupportedTransferLocationException,
                        destLocation.Type));
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                this.reader?.Dispose();

                this.writer?.Dispose();

                foreach (var transferData in this.SharedTransferData.AvailableData.Values)
                {
                    transferData.Dispose();
                }

                this.SharedTransferData.AvailableData.Clear();
            }
        }
    }
}
