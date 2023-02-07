//------------------------------------------------------------------------------
// <copyright file="FlatDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.Azure.Storage.File;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Runtime.ExceptionServices;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a flat directory transfer operation.
    /// In a flat directory transfer, the enumeration only returns file entries and it only transfers files under the directory.
    /// 
    /// In a hierarchy directory transfer, the enumeration also returns directory entries, 
    /// it transfers files under the directory and also handles opertions on directories.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif
    internal class FlatDirectoryTransfer : DirectoryTransfer
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
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        protected SerializableListContinuationToken enumerateContinuationToken;

        /// <summary>
        /// Lock object for enumeration continuation token.
        /// </summary>
        private object lockEnumerateContinuationToken = new object();

        /// <summary>
        /// Timeout used in reset event waiting.
        /// </summary>
        private TimeSpan EnumerationWaitTimeOut = TimeSpan.FromSeconds(10);

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
        private long outstandingTasks;

        private ReaderWriterLockSlim progressUpdateLock = new ReaderWriterLockSlim();

        /// <summary>
        /// Job queue to invoke ShouldTransferCallback.
        /// </summary>
        private TaskQueue<Tuple<SingleObjectTransfer, TransferEntry>> shouldTransferQueue
            = new TaskQueue<Tuple<SingleObjectTransfer, TransferEntry>>(TransferManager.Configurations.ParallelOperations * Constants.ListSegmentLengthMultiplier);

        /// <summary>
        /// Storres sub transfers.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
        private TransferCollection<SingleObjectTransfer> serializeSubTransfers;
#endif
        private TransferCollection<SingleObjectTransfer> subTransfers;

        private TaskCompletionSource<object> allTransfersCompleteSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="FlatDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public FlatDirectoryTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
            this.subTransfers = new TransferCollection<SingleObjectTransfer>();
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="FlatDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected FlatDirectoryTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.enumerateContinuationToken = (SerializableListContinuationToken)info.GetValue(ListContinuationTokenName, typeof(SerializableListContinuationToken));

            if (context.Context is StreamJournal)
            {
                this.subTransfers = new TransferCollection<SingleObjectTransfer>();
            }
            else
            {
                this.subTransfers = (TransferCollection<SingleObjectTransfer>)info.GetValue(SubTransfersName, typeof(TransferCollection<SingleObjectTransfer>));
            }
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }
#endif // BINARY_SERIALIZATION

#if !BINARY_SERIALIZATION
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            if (!IsStreamJournal)
            {
                this.serializeSubTransfers = this.subTransfers;
            }
        }

        // Initialize a new MultipleObjectsTransfer object after deserialization
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            // Constructors and field initializers are not called by DCS, so initialize things here
            progressUpdateLock = new ReaderWriterLockSlim();
            lockEnumerateContinuationToken = new object();
            EnumerationWaitTimeOut = TimeSpan.FromSeconds(10);

            if (!IsStreamJournal)
            {
                this.subTransfers = this.serializeSubTransfers;
            }
            else
            {
                this.subTransfers = new TransferCollection<SingleObjectTransfer>();
            }

            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;

            // DataContractSerializer doesn't invoke object's constructor, we should initialize member variables here.
            shouldTransferQueue 
                = new TaskQueue<Tuple<SingleObjectTransfer, TransferEntry>>(TransferManager.Configurations.ParallelOperations * Constants.ListSegmentLengthMultiplier);
        }
#endif
        /// <summary>
        /// Initializes a new instance of the <see cref="FlatDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="FlatDirectoryTransfer"/> object.</param>
        private FlatDirectoryTransfer(FlatDirectoryTransfer other)
            :base(other)
        {
            other.progressUpdateLock?.EnterWriteLock();
            this.ProgressTracker = other.ProgressTracker.Copy();
            lock (other.lockEnumerateContinuationToken)
            {
                // copy enumerator
                this.enumerateContinuationToken = other.enumerateContinuationToken;

                // copy transfers
                this.subTransfers = other.subTransfers.Copy();
            }
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
            other.progressUpdateLock?.ExitWriteLock();
        }


#if BINARY_SERIALIZATION
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

            if (!(context.Context is StreamJournal))
            {
                // serialize sub transfers
                info.AddValue(SubTransfersName, this.subTransfers, typeof(TransferCollection<SingleObjectTransfer>));
            }
        }
#endif // BINARY_SERIALIZATION

        public override Transfer Copy()
        {
            return new FlatDirectoryTransfer(this);
        }

        public override async Task ExecuteInternalAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            this.ResetExecutionStatus();

            try
            {
                Task listTask = Task.Run(() => this.ListNewTransfers(cancellationToken));

                await Task.Run(() => { this.EnumerateAndTransfer(scheduler, cancellationToken); });

                await listTask;
            }
            finally
            {
                // wait for outstanding transfers to complete
                await allTransfersCompleteSource.Task;
            }

            if (this.enumerateException != null)
            {
                throw this.enumerateException;
            }

            this.ProgressTracker.AddBytesTransferred(0);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                SpinWait sw = new SpinWait();
                while (Interlocked.Read(ref this.outstandingTasks) != 0)
                {
                    sw.SpinOnce();
                }

                if (this.enumerationResetEvent != null)
                {
                    this.enumerationResetEvent.Dispose();
                    this.enumerationResetEvent = null;
                }

                if (this.shouldTransferQueue != null)
                {
                    this.shouldTransferQueue.Dispose();
                    this.shouldTransferQueue = null;
                }

                if (null != this.progressUpdateLock)
                {
                    this.progressUpdateLock.Dispose();
                    this.progressUpdateLock = null;
                }
            }

            base.Dispose(disposing);
        }

        private void ListNewTransfers(CancellationToken cancellationToken)
        {
            // list new transfers
            if (this.enumerateContinuationToken != null)
            {
                this.SourceEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
            }

            ShouldTransferCallbackAsync shouldTransferCallback = this.DirectoryContext?.ShouldTransferCallbackAsync;

            try
            {
                var enumerator = this.SourceEnumerator.EnumerateLocation(cancellationToken).GetEnumerator();

                while (true)
                {
                    Utils.CheckCancellation(cancellationToken);

                    if (!enumerator.MoveNext())
                    {
                        break;
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

                    this.shouldTransferQueue.EnqueueJob(async () =>
                    {
                        try
                        {
                            SingleObjectTransfer candidate = this.CreateTransfer(entry);

                            bool shouldTransfer = shouldTransferCallback == null || await shouldTransferCallback(candidate.Source.Instance, candidate.Destination.Instance);

                            return new Tuple<SingleObjectTransfer, TransferEntry>(shouldTransfer ? candidate : null, entry);
                        }
                        catch (Exception ex)
                        {
                            throw new TransferException(TransferErrorCode.FailToEnumerateDirectory, string.Format(CultureInfo.CurrentCulture, "Error happens when handling entry {0}: {1}", entry.ToString(), ex.Message), ex);
                        }
                    });
                }
            }
            finally
            {
                this.shouldTransferQueue.CompleteAdding();
            }
        }

        private IEnumerable<SingleObjectTransfer> AllTransfers(CancellationToken cancellationToken)
        {
            if (null == this.Journal)
            {
                // return all existing transfers in subTransfers
                foreach (var transfer in this.subTransfers.GetEnumerator())
                {
                    Utils.CheckCancellation(cancellationToken);
                    transfer.Context = this.Context;

                    this.UpdateTransfer(transfer);
                    yield return transfer as SingleObjectTransfer;
                }
            }
            else
            {
                foreach (var transfer in this.Journal.ListSubTransfers())
                {
                    Utils.CheckCancellation(cancellationToken);
                    transfer.Context = this.Context;

                    this.UpdateTransfer(transfer);

                    this.subTransfers.AddTransfer(transfer, false);
                    yield return transfer;
                }
            }

            while (true)
            {
                Utils.CheckCancellation(cancellationToken);

                Tuple<SingleObjectTransfer, TransferEntry> pair;
                try
                {
                    pair = this.shouldTransferQueue.DequeueResult();
                }
                catch (InvalidOperationException)
                {
                    // Task queue is empty and CompleteAdding. No more transfer to dequeue.
                    break;
                }
                catch (AggregateException aggregateException)
                {
                    // Unwrap the AggregateException.
                    ExceptionDispatchInfo.Capture(aggregateException.Flatten().InnerExceptions[0]).Throw();

                    break;
                }


                SingleObjectTransfer transfer = pair.Item1;
                TransferEntry entry = pair.Item2;

                lock (this.lockEnumerateContinuationToken)
                {
                    if (null != transfer)
                    {
                        this.subTransfers.AddTransfer(transfer);
                    }
                    this.enumerateContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                }

                if (null != transfer)
                {
                    this.Journal?.AddSubtransfer(transfer);
                }

                this.Journal?.UpdateJournalItem(this);

                if (null == transfer)
                {
                    continue;
                }

                try
                {
                    this.CreateParentDirectory(transfer);
                }
                catch (Exception ex)
                {
                    transfer.OnTransferFailed(ex);

                    // Don't keep failed transfers in memory if they can be persisted to a journal.
                    if (null != this.Journal)
                    {
                        this.subTransfers.RemoveTransfer(transfer);
                    }
                    continue;
                }

#if DEBUG
                Utils.HandleFaultInjection(entry.RelativePath, transfer);
#endif

                yield return transfer;
            }
        }

        private void EnumerateAndTransfer(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref this.outstandingTasks);

            try
            {
                foreach (var transfer in this.AllTransfers(cancellationToken))
                {
                    this.CheckAndPauseEnumeration(cancellationToken);
                    transfer.UpdateProgressLock(this.progressUpdateLock);
                    transfer.ShouldTransferChecked = true;
                    this.DoTransfer(transfer, scheduler, cancellationToken);
                }
            }
            catch (StorageException e)
            {
                throw new TransferException(TransferErrorCode.FailToEnumerateDirectory, e.GetExceptionMessage(), e);
            }
            finally
            {
                if (Interlocked.Decrement(ref this.outstandingTasks) == 0)
                {
                    // make sure transfer terminiate when there is no subtransfer.
                    this.allTransfersCompleteSource.SetResult(null);
                }
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


        private async void DoTransfer(Transfer transfer, TransferScheduler scheduler, CancellationToken cancellationToken)
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
                    // Don't keep the failed transferring in memory, if the checkpoint is persist to a streamed journal,
                    // instead, should only keep them in the streamed journal.
                    if ((!hasError)
                        || (null != this.Journal))
                    {
                        this.subTransfers.RemoveTransfer(transfer);
                    }

                    this.enumerationResetEvent.Set();

                    if (Interlocked.Decrement(ref this.outstandingTasks) == 0)
                    {
                        // all transfers are done
                        this.allTransfersCompleteSource.SetResult(null);
                    }
                }
            }
        }
    }
}
