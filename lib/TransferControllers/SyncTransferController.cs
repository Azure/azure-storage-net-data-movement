//------------------------------------------------------------------------------
// <copyright file="SyncTransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal class SyncTransferController : TransferControllerBase
    {
        private TransferReaderWriterBase reader;
        private TransferReaderWriterBase writer;

        public SyncTransferController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
            if (null == transferScheduler)
            {
                throw new ArgumentNullException("transferScheduler");
            }

            if (null == transferJob)
            {
                throw new ArgumentNullException("transferJob");
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
            
            reader = this.GetReader(transferJob.Source);
            writer = this.GetWriter(transferJob.Destination);
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

        public override bool HasWork
        {
            get
            {
                var hasWork = (!this.reader.PreProcessed && this.reader.HasWork) || (this.reader.PreProcessed && this.writer.HasWork) || (this.writer.PreProcessed && this.reader.HasWork);
                return !this.ErrorOccurred && hasWork;
            }
        }

        protected override async Task<bool> DoWorkInternalAsync()
        {
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

        private TransferReaderWriterBase GetReader(TransferLocation sourceLocation)
        {
            switch (sourceLocation.TransferLocationType)
            {
                case TransferLocationType.Stream:
                    return new StreamedReader(this.Scheduler, this, this.CancellationToken);
                case TransferLocationType.FilePath:
                    return new StreamedReader(this.Scheduler, this, this.CancellationToken);
                case TransferLocationType.AzureBlob:
                    if (sourceLocation.Blob is CloudPageBlob)
                    {
                        return new PageBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (sourceLocation.Blob is CloudBlockBlob)
                    {
                        return new BlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (sourceLocation.Blob is CloudAppendBlob)
                    {
                        return new BlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else 
                    {
                        throw new InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.UnsupportedBlobTypeException,
                            sourceLocation.Blob.BlobType));
                    }
                case TransferLocationType.AzureFile:
                    return new CloudFileReader(this.Scheduler, this, this.CancellationToken);
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.UnsupportedTransferLocationException,
                        sourceLocation.TransferLocationType));
            }
        }

        private TransferReaderWriterBase GetWriter(TransferLocation destLocation)
        {
            switch (destLocation.TransferLocationType)
            {
                case TransferLocationType.Stream:
                    return new StreamedWriter(this.Scheduler, this, this.CancellationToken);
                case TransferLocationType.FilePath:
                    return new StreamedWriter(this.Scheduler, this, this.CancellationToken);
                case TransferLocationType.AzureBlob:
                    if (destLocation.Blob is CloudPageBlob)
                    {
                        return new PageBlobWriter(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (destLocation.Blob is CloudBlockBlob)
                    {
                        return new BlockBlobWriter(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (destLocation.Blob is CloudAppendBlob)
                    {
                        return new AppendBlobWriter(this.Scheduler, this, this.CancellationToken);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.UnsupportedBlobTypeException,
                            destLocation.Blob.BlobType));
                    }
                case TransferLocationType.AzureFile:
                    return new CloudFileWriter(this.Scheduler, this, this.CancellationToken);
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.UnsupportedTransferLocationException,
                        destLocation.TransferLocationType));
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                if (null != this.reader)
                {
                    this.reader.Dispose();
                }

                if (null != this.writer)
                {
                    this.writer.Dispose();
                }

                foreach(var transferData in this.SharedTransferData.AvailableData.Values)
                {
                    transferData.Dispose();
                }

                this.SharedTransferData.AvailableData.Clear();
            }
        }
    }
}
