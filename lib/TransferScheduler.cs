//-----------------------------------------------------------------------------
// <copyright file="TransferScheduler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers;

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
        /// CancellationToken source.
        /// </summary>
        private CancellationTokenSource cancellationTokenSource =
            new CancellationTokenSource();

        /// <summary>
        /// Transfer options that this manager will pass to transfer controllers.
        /// </summary>
        private TransferConfigurations transferOptions;
        
        /// <summary>
        /// Transfer pacer which is used to adjust adaptive parameters and performance logging
        /// </summary>
        private TransferPacer pacer;

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

        private volatile int ongoingTasks;

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
            
            this.pacer = new TransferPacer { Configurations = this.transferOptions };

            this.internalControllerQueue = new ConcurrentQueue<ITransferController>();
            this.controllerQueue = new BlockingCollection<ITransferController>(
                this.internalControllerQueue);
            this.memoryManager = new MemoryManager(
                this.transferOptions.MaximumCacheSize,
                this.transferOptions.MemoryChunkSize);

            this.randomGenerator = new Random();

            this.ongoingTasks = 0;

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
        internal TransferConfigurations TransferOptions
        {
            get
            {
                return this.transferOptions;
            }
        }
        
        /// <summary>
        /// Gets the tranfser pacer which controller auto-tuned paramters and logging
        /// </summary>
        internal TransferPacer Pacer
        {
            get
            {
                return this.pacer;
            }
        }

        internal CancellationTokenSource CancellationTokenSource
        {
            get
            {
                return this.cancellationTokenSource;
            }
        }

        internal MemoryManager MemoryManager
        {
            get
            {
                return this.memoryManager;
            }
        }

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
                throw new ArgumentNullException("job");
            }

            lock (this.disposeLock)
            {
                this.CheckDisposed();

                return ExecuteJobInternalAsync(job, cancellationToken);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Instances will be disposed in other place.")]
        private async Task ExecuteJobInternalAsync(
            TransferJob job,
            CancellationToken cancellationToken)
        {
            Debug.Assert(
                job.Status == TransferJobStatus.NotStarted ||
                job.Status == TransferJobStatus.Monitor ||
                job.Status == TransferJobStatus.Transfer);

            TransferControllerBase controller = null;
            switch (job.Transfer.TransferMethod)
            {
                case TransferMethod.SyncCopy:
                    SyncTransferController syncController;
                    controller = syncController = new SyncTransferController(this, job, cancellationToken);
                    pacer.Register(syncController);
                    break;

                case TransferMethod.AsyncCopy:
                    controller = AsyncCopyController.CreateAsyncCopyController(this, job, cancellationToken);
                    break;
            }

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
                && activeItems.Count < this.transferOptions.ParallelOperations)
            {
                if (activeItems.Count >= this.transferOptions.ParallelOperations)
                {
                    return;
                }

                ITransferController transferItem = null;

                try
                {
                    if (!collection.TryTake(out transferItem)
                        || null == transferItem)
                    {
                        return;
                    }
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                activeItems.TryAdd(transferItem, null);
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
                        (!this.controllerQueue.IsCompleted || this.activeControllerItems.Any()))
                    {
                        FillInQueue(
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
            this.activeControllerItems.TryRemove(transferController, out dummy);
            
            var syncController = transferController as SyncTransferController;
            if (syncController != null)
            {
                pacer.Deregister(syncController);
            }
        }

        private bool DoWorkFrom(
            ConcurrentDictionary<ITransferController, object> activeItems)
        {
            // Filter items with work only.
            List<KeyValuePair<ITransferController, object>> activeItemsWithWork =
                new List<KeyValuePair<ITransferController, object>>(
                    activeItems.Where(item => item.Key.HasWork && !item.Key.IsFinished));

            if (0 != activeItemsWithWork.Count)
            {
                // Select random item and get work delegate.
                int idx = this.randomGenerator.Next(activeItemsWithWork.Count);
                ITransferController transferController = activeItemsWithWork[idx].Key;

                if (Interlocked.Increment(ref this.ongoingTasks) <= TransferManager.Configurations.ParallelOperations)
                {
                    DoControllerWork(transferController);
                    return true;
                }
                else
                {
                    Interlocked.Decrement(ref this.ongoingTasks);
                    return false;
                }
            }

            return false;
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
            }

            if (finished)
            {
                this.FinishedWorkItem(controller);
            }
        }
    }
}
