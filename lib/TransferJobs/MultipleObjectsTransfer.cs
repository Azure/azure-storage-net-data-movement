//------------------------------------------------------------------------------
// <copyright file="MultipleObjectsTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Represents a multiple objects transfer operation.
    /// </summary>
    [Serializable]
    internal abstract class MultipleObjectsTransfer : Transfer
    {
        /// <summary>
        /// Serialization field name for transfer enumerator list continuation token.
        /// </summary>
        private const string ListContinuationTokenName = "ListContinuationToken";

        /// <summary>
        /// Serialization field name for sub transfers.
        /// </summary>
        private const string SubTransfersName = "SubTransfers";

        /// <summary>
        /// List continuation token from which enumeration begins.
        /// </summary>
        private SerializableListContinuationToken enumerateContinuationToken;

        /// <summary>
        /// Lock object for enumeration continuation token.
        /// </summary>
        private object lockEnumerateContinuationToken = new object();

        /// <summary>
        /// Timeout used in reset event waiting.
        /// </summary>
        private readonly TimeSpan EnumerationWaitTimeOut = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Used to block enumeration when have enumerated enough transfer entries.
        /// </summary>
        private AutoResetEvent enumerationResetEvent;

        /// <summary>
        /// Stores enumerate exception.
        /// </summary>
        private Exception enumerateException;

        /// <summary>
        /// Number of outstandings tasks started by this transfer.
        /// </summary>
        private int outstandingTasks;

        /// <summary>
        /// Storres sub transfers.
        /// </summary>
        private TransferCollection subTransfers;

        private TaskCompletionSource<object> allTransfersCompleteSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleObjectsTransfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public MultipleObjectsTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
            this.subTransfers = new TransferCollection();
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleObjectsTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected MultipleObjectsTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.enumerateContinuationToken = (SerializableListContinuationToken)info.GetValue(ListContinuationTokenName, typeof(SerializableListContinuationToken));
            this.subTransfers = (TransferCollection)info.GetValue(SubTransfersName, typeof(TransferCollection));
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleObjectsTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="MultipleObjectsTransfer"/> object.</param>
        protected MultipleObjectsTransfer(MultipleObjectsTransfer other)
            : base(other)
        {
            lock (other.lockEnumerateContinuationToken)
            {
                // copy enumerator
                this.enumerateContinuationToken = other.enumerateContinuationToken;

                // copy transfers
                this.subTransfers = other.subTransfers.Copy();
            }

            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

        /// <summary>
        /// Gets or sets the transfer enumerator for source location
        /// </summary>
        public ITransferEnumerator SourceEnumerator
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum transfer concurrency
        /// </summary>
        public int MaxTransferConcurrency
        {
            get;
            set;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            // serialize enumerator
            info.AddValue(ListContinuationTokenName, this.enumerateContinuationToken, typeof(SerializableListContinuationToken));

            // serialize sub transfers
            info.AddValue(SubTransfersName, this.subTransfers, typeof(TransferCollection));
        }

        /// <summary>
        /// Execute the transfer asynchronously.
        /// </summary>
        /// <param name="scheduler">Transfer scheduler</param>
        /// <param name="cancellationToken">Token that can be used to cancel the transfer.</param>
        /// <returns>A task representing the transfer operation.</returns>
        public override async Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            this.ResetExecutionStatus();

            this.Destination.Validate();

            Task enumerateTask = Task.Run(() => { this.EnumerateAndTransfer(scheduler, cancellationToken); });
            await enumerateTask;

            // wait for outstanding transfers to complete
            await allTransfersCompleteSource.Task;

            if (this.enumerateException != null)
            {
                throw this.enumerateException;
            }
            else if (this.subTransfers.Count != 0)
            {
                string errorMessage = string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.SubTransferFailsException,
                    this.subTransfers.Count);

                throw new TransferException(TransferErrorCode.SubTransferFails, errorMessage);
            }
        }

        protected abstract Transfer CreateTransfer(TransferEntry entry);

        protected abstract void UpdateTransfer(Transfer transfer);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this.enumerationResetEvent != null)
                {
                    this.enumerationResetEvent.Dispose();
                    this.enumerationResetEvent = null;
                }
            }

            base.Dispose(disposing);
        }

        private IEnumerable<Transfer> AllTransfers(CancellationToken cancellationToken)
        {
            // return all existing transfers in subTransfers
            foreach (var transfer in this.subTransfers.GetEnumerator())
            {
                Utils.CheckCancellation(cancellationToken);
                transfer.Context = this.Context;
                transfer.ContentType = this.ContentType;

                this.UpdateTransfer(transfer);
                yield return transfer;
            }

            // list new transfers
            if (this.enumerateContinuationToken != null)
            {
                this.SourceEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
            }

            var enumerator = this.SourceEnumerator.EnumerateLocation(cancellationToken).GetEnumerator();

            while (true)
            {
                Utils.CheckCancellation(cancellationToken);

                // lock enumerator
                if (!enumerator.MoveNext())
                {
                    yield break;
                }

                TransferEntry entry = enumerator.Current;
                ErrorEntry errorEntry = entry as ErrorEntry;
                if (errorEntry != null)
                {
                    TransferException exception = errorEntry.Exception as TransferException;
                    if (null != exception)
                    {
                        throw exception;
                    }
                    else
                    {
                        throw new TransferException(
                            TransferErrorCode.FailToEnumerateDirectory,
                            errorEntry.Exception.GetExceptionMessage(),
                            errorEntry.Exception);
                    }
                }

                Transfer transfer = this.CreateTransfer(entry);

                lock (this.lockEnumerateContinuationToken)
                {
                    this.subTransfers.AddTransfer(transfer);
                    this.enumerateContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                }

                yield return transfer;
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Needed to ensure exceptions are not thrown on threadpool threads.")]
        private void EnumerateAndTransfer(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref this.outstandingTasks);

            try
            {
                foreach (var transfer in this.AllTransfers(cancellationToken))
                {
                    this.CheckAndPauseEnumeration(cancellationToken);

                    Task unused = this.DoTransfer(transfer, scheduler, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                // exception happens when enumerating source
                this.enumerateException = ex;
            }

            if (Interlocked.Decrement(ref this.outstandingTasks) == 0)
            {
                // make sure transfer terminiate when there is no subtransfer.
                this.allTransfersCompleteSource.SetResult(null);
            }
        }

        private void CheckAndPauseEnumeration(CancellationToken cancellationToken)
        {
            // -1 because there's one outstanding task for list while this method is called.
            if ((this.outstandingTasks - 1) > this.MaxTransferConcurrency)
            {
                while (!this.enumerationResetEvent.WaitOne(EnumerationWaitTimeOut)
                    && !cancellationToken.IsCancellationRequested)
                {
                }
            }
        }

        private void ResetExecutionStatus()
        {
            if (this.enumerationResetEvent != null)
            {
                this.enumerationResetEvent.Dispose();
            }

            this.enumerationResetEvent = new AutoResetEvent(true);

            this.enumerateException = null;

            this.allTransfersCompleteSource = new TaskCompletionSource<object>();

            this.outstandingTasks = 0;
        }


        private async Task DoTransfer(Transfer transfer, TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            using (transfer)
            {
                bool hasError = false;

                Interlocked.Increment(ref this.outstandingTasks);

                try
                {
                    await transfer.ExecuteAsync(scheduler, cancellationToken);
                }
                catch
                {
                    // catch exception thrown from sub-transfer as it's already recorded
                    hasError = true;
                }
                finally
                {
                    if (!hasError)
                    {
                        this.subTransfers.RemoveTransfer(transfer);
                    }

                    if (Interlocked.Decrement(ref this.outstandingTasks) == 0)
                    {
                        // all transfers are done
                        this.allTransfersCompleteSource.SetResult(null);
                    }

                    this.enumerationResetEvent.Set();
                }
            }
        }
    }
}
