//------------------------------------------------------------------------------
// <copyright file="ITransferReaderWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal abstract class TransferReaderWriterBase : IDisposable
    {
        protected TransferReaderWriterBase(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
        {
            this.Scheduler = scheduler;
            this.Controller = controller;
            this.CancellationToken = cancellationToken;
        }

        /// <summary>
        /// Gets a value indicating whether it finished preprocess.
        /// For producer, preprocess is to validate source and fetch block list/page ranges;
        /// For consumer, preprocess is to open or create destination.
        /// </summary>
        public virtual bool PreProcessed
        {
            get;
            protected set;
        }

        public abstract bool HasWork
        {
            get;
        }

        public abstract bool IsFinished
        {
            get;
        }

        protected TransferScheduler Scheduler
        {
            get;
            private set;
        }

        protected SyncTransferController Controller
        {
            get;
            private set;
        }

        protected SharedTransferData SharedTransferData
        {
            get
            {
                return this.Controller?.SharedTransferData;
            }
        }

        protected CancellationToken CancellationToken
        {
            get;
            private set;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        protected void NotifyStarting()
        {
            this.Controller.StartCallbackHandler();
        }

        protected void NotifyFinished(Exception ex)
        {
            this.Controller.FinishCallbackHandler(ex);
        }

        public abstract Task DoWorkInternalAsync();

        public TransferData GetFirstAvailable()
        {
            TransferData transferData = null;
            var transferDatas = this.SharedTransferData.AvailableData.Values;

            if (transferDatas.Any())
            {
                transferData = transferDatas.First();
                TransferData tempData;
                this.SharedTransferData.AvailableData.TryRemove(transferData.StartOffset, out tempData);
                return transferData;
            }

            return null;
        }
    }
}
