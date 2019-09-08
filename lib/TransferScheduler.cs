//-----------------------------------------------------------------------------
// <copyright file="TransferScheduler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferControllers;

    /// <summary>
    /// TransferScheduler class, used for  transferring Microsoft Azure
    /// Storage objects.
    /// </summary>
    internal sealed class TransferScheduler : IDisposable
    {
        /// <summary>
        /// Main collection of transfer controllers.
        /// </summary>
        private BlockingCollection<ITransferController> controllerQueue;

        /// <summary>
        /// Internal queue for the main controllers collection.
        /// </summary>
        private ConcurrentQueue<ITransferController> internalControllerQueue;

        /// <summary>
        /// A buffer from which we select a transfer controller and add it into 
        /// active tasks when the bucket of active tasks is not full.
        /// </summary>
        private ConcurrentDictionary<ITransferController, object> activeControllerItems =
            new ConcurrentDictionary<ITransferController, object>();

        /// <summary>
        /// Active controller item count used to help activeControllerItems statistics.
        /// </summary>
        private long activeControllerItemCount = 0;

        /// <summary>
        /// Active controller item prefetch ratio, used to set how much controller item could be prefetched during scheduling controllers.
        /// </summary>
        private const double ActiveControllerItemPrefetchRatio = 1.2; // TODO: further tune the prefetch ratio.

        /// <summary>
        /// Max active controller item count used to limit candidate controller items to be scheduled in parallel.
        /// Note: ParallelOperations could be changing.
        /// </summary>
        private static int MaxActiveControllerItemCount => (int)(TransferManager.Configurations.ParallelOperations * ActiveControllerItemPrefetchRatio);

        /// <summary>
        /// Ongoing task used to control ongoing tasks dispatching.
        /// </summary>
        private int ongoingTasks;

        /// <summary>
        /// Ongoing task sempahore used to help ongoing task dispatching.
        /// </summary>
        private ManualResetEventSlim ongoingTaskEvent;

        /// <summary>
        /// Max controller work items count to schedule per scheduling round.
        /// </summary>
        private const int MaxControllerItemCountToScheduleEachRound = 100;

        /// <summary>
        /// CancellationToken source.
        /// </summary>
        private CancellationTokenSource cancellationTokenSource =
            new CancellationTokenSource();

        /// <summary>
        /// Transfer options that this manager will pass to transfer controllers.
        /// </summary>
        private TransferConfigurations transferOptions;

        /// <summary>
        /// Wait handle event for completion.
        /// </summary>
        private ManualResetEventSlim controllerResetEvent =
            new ManualResetEventSlim();

        /// <summary>
        /// A pool of memory buffer objects, used to limit total consumed memory.
        /// </summary>
        private MemoryManager memoryManager;

        /// <summary>
        /// Random object to generate random numbers.
        /// </summary>
        private Random randomGenerator;

        /// <summary>
        /// Used to lock disposing to avoid race condition between different disposing and other method calls.
        /// </summary>
        private object disposeLock = new object();

        /// <summary>
        /// Indicate whether the instance has been disposed.
        /// </summary>
        private bool isDisposed = false;

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="TransferScheduler" /> class.
        /// </summary>        
        public TransferScheduler()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="TransferScheduler" /> class.
        /// </summary>
        /// <param name="options">BlobTransfer options.</param>
        public TransferScheduler(TransferConfigurations options)
        {
            // If no options specified create a default one.
            this.transferOptions = options ?? new TransferConfigurations();

            this.internalControllerQueue = new ConcurrentQueue<ITransferController>();
            this.controllerQueue = new BlockingCollection<ITransferController>(
                this.internalControllerQueue);
            this.memoryManager = new MemoryManager(
                this.transferOptions.MaximumCacheSize,
                this.transferOptions.MemoryChunkSize);

            this.randomGenerator = new Random();

            this.ongoingTasks = 0;

            this.ongoingTaskEvent = new ManualResetEventSlim(false);

            this.StartSchedule();
        }

        /// <summary>
        /// Finalizes an instance of the 
        /// <see cref="TransferScheduler" /> class.
        /// </summary>
        ~TransferScheduler()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets the transfer options that this manager will pass to
        /// transfer controllers.
        /// </summary>
        internal TransferConfigurations TransferOptions => this.transferOptions;

        internal CancellationTokenSource CancellationTokenSource => this.cancellationTokenSource;

        internal MemoryManager MemoryManager => this.memoryManager;

        /// <summary>
        /// Public dispose method to release all resources owned.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Execute a transfer job asynchronously.
        /// </summary>
        /// <param name="job">Transfer job to be executed.</param>
        /// <param name="cancellationToken">Token used to notify the job that it should stop.</param>
        public Task ExecuteJobAsync(
            TransferJob job,
            CancellationToken cancellationToken)
        {
            if (null == job)
            {
                throw new ArgumentNullException(nameof(job));
            }

            lock (this.disposeLock)
            {
                this.CheckDisposed();

                return this.ExecuteJobInternalAsync(job, cancellationToken);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Instances will be disposed in other place.")]
        private async Task ExecuteJobInternalAsync(
            TransferJob job,
            CancellationToken cancellationToken)
        {
            Debug.Assert(
                job.Status == TransferJobStatus.NotStarted ||
                job.Status == TransferJobStatus.SkippedDueToShouldNotTransfer ||
                job.Status == TransferJobStatus.Monitor ||
                job.Status == TransferJobStatus.Transfer);

            if (job.Status == TransferJobStatus.SkippedDueToShouldNotTransfer)
            {
                return;
            }

            TransferControllerBase controller = GenerateTransferConstroller(this, job, cancellationToken);

            Utils.CheckCancellation(this.cancellationTokenSource.Token);
            this.controllerQueue.Add(controller, this.cancellationTokenSource.Token);

            try
            {
                await controller.TaskCompletionSource.Task;
            }

#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var storageException = ex as StorageException ?? ex.InnerException as StorageException;

                if (storageException.InnerException is OperationCanceledException)
                {
                    throw storageException.InnerException;
                }

                throw new TransferException(TransferErrorCode.Unknown,
                    Resources.UncategorizedException,
                    storageException);
            }
#else
            catch (StorageException se)
            {
                throw new TransferException(
                    TransferErrorCode.Unknown,
                    Resources.UncategorizedException,
                    se);
            }

#endif
            finally
            {
                controller.Dispose();
            }
        }

        private void FillInQueue(
            ConcurrentDictionary<ITransferController, object> activeItems,
            BlockingCollection<ITransferController> collection,
            CancellationToken token)
        {
            while (!token.IsCancellationRequested
                && Interlocked.Read(ref this.activeControllerItemCount) < MaxActiveControllerItemCount)
            {
                ITransferController transferItem = null;

                try
                {
                    if (this.activeControllerItemCount <= 0)
                    {
                        transferItem = collection.Take(this.cancellationTokenSource.Token);

                        if (null == transferItem)
                        {
                            return;
                        }
                    }
                    else
                    {
                        if (!collection.TryTake(out transferItem)
                            || null == transferItem)
                        {
                            return;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (InvalidOperationException)
                {
                    // This kind of exception will be thrown when the BlockingCollection is marked as complete for adding, or is disposed. 
                    return;
                }

                if (activeItems.TryAdd(transferItem, null))
                {
                    Interlocked.Increment(ref this.activeControllerItemCount);
                }
            }
        }

        /// <summary>
        /// Blocks until the queue is empty and all transfers have been 
        /// completed.
        /// </summary>
        private void WaitForCompletion()
        {
            this.controllerResetEvent.Wait();
        }

        /// <summary>
        /// Cancels any remaining queued work.
        /// </summary>
        private void CancelWork()
        {
            this.cancellationTokenSource.Cancel();
            this.controllerQueue.CompleteAdding();

            // Move following to Cancel method.
            // there might be running "work" when the transfer is cancelled.
            // wait until all running "work" is done.
            SpinWait sw = new SpinWait();
            while (this.ongoingTasks != 0)
            {
                sw.SpinOnce();
            }

            this.controllerResetEvent.Set();
        }

        /// <summary>
        /// Private dispose method to release managed/unmanaged objects.
        /// If disposing is true clean up managed resources as well as 
        /// unmanaged resources.
        /// If disposing is false only clean up unmanaged resources.
        /// </summary>
        /// <param name="disposing">Indicates whether or not to dispose 
        /// managed resources.</param>
        private void Dispose(bool disposing)
        {
            if (!this.isDisposed)
            {
                lock (this.disposeLock)
                {
                    // We got the lock, isDisposed is true, means that the disposing has been finished.
                    if (this.isDisposed)
                    {
                        return;
                    }

                    this.isDisposed = true;

                    this.CancelWork();
                    this.WaitForCompletion();

                    if (disposing)
                    {
                        if (null != this.controllerQueue)
                        {
                            this.controllerQueue.Dispose();
                            this.controllerQueue = null;
                        }

                        if (null != this.cancellationTokenSource)
                        {
                            this.cancellationTokenSource.Dispose();
                            this.cancellationTokenSource = null;
                        }

                        if (null != this.controllerResetEvent)
                        {
                            this.controllerResetEvent.Dispose();
                            this.controllerResetEvent = null;
                        }

                        if (null != this.ongoingTaskEvent)
                        {
                            this.ongoingTaskEvent.Dispose();
                            this.ongoingTaskEvent = null;
                        }

                        this.memoryManager = null;
                    }
                }
            }
            else
            {
                this.WaitForCompletion();
            }
        }

        private void CheckDisposed()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException("TransferScheduler");
            }
        }

        private void StartSchedule()
        {
            Task.Run(() =>
                {
                    SpinWait sw = new SpinWait();
                    while (!this.cancellationTokenSource.Token.IsCancellationRequested &&
                        (!this.controllerQueue.IsCompleted || Interlocked.Read(ref this.activeControllerItemCount) != 0))
                    {
                        this.FillInQueue(
                            this.activeControllerItems,
                            this.controllerQueue,
                            this.cancellationTokenSource.Token);

                        if (!this.cancellationTokenSource.Token.IsCancellationRequested)
                        {
                            // If we don't have the requested amount of active tasks
                            // running, get a task item from any active transfer item
                            // that has work available.
                            if (!this.DoWorkFrom(this.activeControllerItems))
                            {
                                sw.SpinOnce();
                            }
                            else
                            {
                                sw.Reset();
                                continue;
                            }
                        }
                    }
                });
        }

        private void FinishedWorkItem(
            ITransferController transferController)
        {
            object dummy;

            if (this.activeControllerItems.TryRemove(transferController, out dummy))
            {
                Interlocked.Decrement(ref this.activeControllerItemCount);
            }
        }

        private bool DoWorkFrom(
            ConcurrentDictionary<ITransferController, object> activeItems)
        {
            // Filter items with work only.
            // TODO: Optimize scheduling efficiency, get active items with LINQ cost a lot of time.
            List<KeyValuePair<ITransferController, object>> activeItemsWithWork =
            new List<KeyValuePair<ITransferController, object>>(
                activeItems.Where(item => item.Key.HasWork && !item.Key.IsFinished));

            int scheduledItemsCount = 0;

            // In order to save time used to lock/search/addItem ConcurrentDictionary, try to schedule multipe items per DoWorkFrom round. 
            while (0 != activeItemsWithWork.Count
                && scheduledItemsCount < MaxControllerItemCountToScheduleEachRound)
            {
                // Select random item and get work delegate.
                int idx = this.randomGenerator.Next(activeItemsWithWork.Count);
                ITransferController transferController = activeItemsWithWork[idx].Key;

                var scheduledOneItem = false;
                while (!scheduledOneItem)
                {
                    // Note: TransferManager.Configurations.ParallelOperations could be a changing value.
                    if (Interlocked.Increment(ref this.ongoingTasks) <= TransferManager.Configurations.ParallelOperations)
                    {
                        // Note: This is the only place where ongoing task could be scheduled.
                        this.DoControllerWork(transferController);

                        scheduledItemsCount++;
                        activeItemsWithWork.RemoveAt(idx);

                        scheduledOneItem = true;
                    }
                    else
                    {
                        Interlocked.Decrement(ref this.ongoingTasks);
                        this.ongoingTaskEvent.Wait();
                        this.ongoingTaskEvent.Reset();
                    }
                }
            }

            if (scheduledItemsCount > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private async void DoControllerWork(ITransferController controller)
        {
            bool finished = false;
            try
            {
                finished = await controller.DoWorkAsync();
            }
            finally
            {
                Interlocked.Decrement(ref this.ongoingTasks);
                this.ongoingTaskEvent.Set();
            }

            if (finished)
            {
                this.FinishedWorkItem(controller);
            }
        }

        private static TransferControllerBase GenerateTransferConstroller(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
        {
            TransferControllerBase controller = null;

            switch (transferJob.Transfer.TransferMethod)
            {
                case TransferMethod.SyncCopy:
                    controller = new SyncTransferController(transferScheduler, transferJob, cancellationToken);
                    break;

                case TransferMethod.ServiceSideAsyncCopy:
                    controller = CreateAsyncCopyController(transferScheduler, transferJob, cancellationToken);
                    break;

                case TransferMethod.ServiceSideSyncCopy:
                    controller = CreateServiceSideSyncCopyConstroller(transferScheduler, transferJob, cancellationToken);
                    break;

                case TransferMethod.DummyCopy:
                    controller = new DummyTransferController(transferScheduler, transferJob, cancellationToken);
                    break;
            }

            return controller;
        }

        private static ServiceSideSyncCopyController CreateServiceSideSyncCopyConstroller(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
        {
            CloudBlob destinationBlob = transferJob.Destination.Instance as CloudBlob;

            if (null == destinationBlob)
            {
                throw new TransferException(Resources.ServiceSideSyncCopyNotSupportException);
            }

            if (BlobType.PageBlob == destinationBlob.BlobType)
            {
                return new PageBlobServiceSideSyncCopyController(transferScheduler, transferJob, cancellationToken);
            }
            else if (BlobType.AppendBlob == destinationBlob.BlobType)
            {
                return new AppendBlobServiceSideSyncCopyController(transferScheduler, transferJob, cancellationToken);
            }
            else if (BlobType.BlockBlob == destinationBlob.BlobType)
            {
                return new BlockBlobServiceSideSyncCopyController(transferScheduler, transferJob, cancellationToken);
            }
            else
            {
                throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.NotSupportedBlobType, destinationBlob.BlobType));
            }
        }

        private static AsyncCopyController CreateAsyncCopyController(TransferScheduler transferScheduler, TransferJob transferJob, CancellationToken cancellationToken)
        {
            if (transferJob.Destination.Type == TransferLocationType.AzureFile)
            {
                return new FileAsyncCopyController(transferScheduler, transferJob, cancellationToken);
            }

            if (transferJob.Destination.Type == TransferLocationType.AzureBlob)
            {
                return new BlobAsyncCopyController(transferScheduler, transferJob, cancellationToken);
            }

            throw new InvalidOperationException(Resources.CanOnlyCopyToFileOrBlobException);
        }
    }
}
