//------------------------------------------------------------------------------
// <copyright file="TransferControllerBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;

    internal abstract class TransferControllerBase : ITransferController, IDisposable
    {
        /// <summary>
        /// Count of active tasks in this controller.
        /// </summary>
        private int activeTasks;
        
        private volatile bool isFinished = false;

        private object lockOnFinished = new object();

        private int notifiedFinish;

        private object cancelLock = new object();

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private CancellationTokenRegistration transferSchedulerCancellationTokenRegistration;

        private CancellationTokenRegistration userCancellationTokenRegistration;

        /// <summary>
        /// Exception used to be thrown during transfer.
        /// DoWorkAsync can be invoked many times, while the controller only throws out one exception when the last work is done.
        /// This is to save the exception during transfer, the last DoWorkAsync will throw it out to transfer caller.
        /// </summary>
        private Exception transferException = null;

        protected TransferControllerBase(TransferScheduler transferScheduler, TransferJob transferJob, CancellationToken userCancellationToken)
        {
            if (null == transferScheduler)
            {
                throw new ArgumentNullException("transferScheduler");
            }

            if (null == transferJob)
            {
                throw new ArgumentNullException("transferJob");
            }

            this.Scheduler = transferScheduler;
            this.TransferJob = transferJob;

            this.transferSchedulerCancellationTokenRegistration =
                this.Scheduler.CancellationTokenSource.Token.Register(this.CancelWork);

            this.userCancellationTokenRegistration = userCancellationToken.Register(this.CancelWork);
            this.TaskCompletionSource = new TaskCompletionSource<object>();
        }

        ~TransferControllerBase()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets or sets the transfer context for the controller.
        /// </summary>
        public TransferContext TransferContext
        {
            get
            {
                return this.TransferJob.Transfer.Context;
            }
        }

        /// <summary>
        /// Gets whether to force overwrite the destination without existence check.
        /// </summary>
        public bool IsForceOverwrite
        {
            get
            {
                if (this.TransferJob.Transfer.Context == null)
                {
                    return false;
                }

                return this.TransferJob.Transfer.Context.ShouldOverwriteCallbackAsync == TransferContext.ForceOverwrite;
            }
        }


        /// <summary>
        /// Gets or sets a value indicating whether the controller has work available
        /// or not for the calling code. If HasWork is false, while IsFinished
        /// is also false this indicates that there are currently still active
        /// async tasks running. The caller should continue checking if this
        /// controller HasWork available later; once the currently active 
        /// async tasks are done HasWork will change to True, or IsFinished
        /// will be set to True.
        /// </summary>
        public abstract bool HasWork
        {
            get;
        }

        /// <summary>
        /// Gets a value indicating whether this controller is finished with
        /// its transferring task.
        /// </summary>
        public bool IsFinished
        {
            get
            {
                return this.isFinished;
            }
        }

        public TaskCompletionSource<object> TaskCompletionSource
        {
            get;
            set;
        }

        /// <summary>
        /// Gets scheduler object which creates this object.
        /// </summary>
        protected TransferScheduler Scheduler
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets TransferJob related to this controller.
        /// </summary>
        protected TransferJob TransferJob
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the cancellation token to control the controller's work.
        /// </summary>
        protected CancellationToken CancellationToken => this.cancellationTokenSource?.Token ?? CancellationToken.None;

        protected async Task<bool> CheckShouldTransfer()
        {
            var directoryTransferContext = this.TransferContext as DirectoryTransferContext;
            if ((null != directoryTransferContext)
                && (null != directoryTransferContext.ShouldTransferCallbackAsync))
            {
                await Task.Yield();
                bool shouldTransfer = true;

                try
                {
                    shouldTransfer = await directoryTransferContext.ShouldTransferCallbackAsync(this.TransferJob.Source.Instance, this.TransferJob.Destination.Instance);
                }
                catch (Exception ex)
                {
                    this.FinishCallbackHandler(new TransferException(TransferErrorCode.FailedCheckingShouldTransfer, string.Empty, ex));
                    return true;
                }

                if (shouldTransfer)
                {
                    this.TransferJob.Transfer.ShouldTransferChecked = true;
                }
                else
                {
                    this.TransferJob.Status = TransferJobStatus.SkippedDueToShouldNotTransfer;
                    this.FinishCallbackHandler(null);
                    return true;
                }
            }
            else
            {
                this.TransferJob.Transfer.ShouldTransferChecked = true;
            }

            return false;
        }

        /// <summary>
        /// Do work in the controller.
        /// A controller controls the whole transfer from source to destination, 
        /// which could be split into several work items. This method is to let controller to do one of those work items.
        /// There could be several work items to do at the same time in the controller. 
        /// </summary>
        /// <returns>Whether the controller has completed. This is to tell <c>TransferScheduler</c> 
        /// whether the controller can be disposed.</returns>
        public async Task<bool> DoWorkAsync()
        {
            if (!this.HasWork)
            {
                return false;
            }

            bool setFinish = false;
            Exception exception = null;
            this.PreWork();

            try
            {
                Utils.CheckCancellation(this.CancellationToken);
                setFinish = await this.DoWorkInternalAsync();
            }
            catch (Exception ex)
            {
                this.SetErrorState(ex);
                setFinish = true;
                exception = ex;
            }

            bool transferFinished = false;

            if (setFinish)
            {
                transferFinished = this.SetFinishedAndPostWork();
            }
            else
            {
                transferFinished = this.PostWork();
            }

            if (null != exception)
            {
                // There could be multiple exception thrown out during transfer for DoWorkAsync can be invoked multiple times,
                // while the controller only throws out one of the exceptions for now, no matter which one.
                transferException = exception;
            }

            // Only throw out exception or set Task's result when the transfer is finished and there's no active operation.
            if (transferFinished)
            {
                this.FinishCallbackHandler(transferException);
            }

            return transferFinished;
        }

        /// <summary>
        /// Cancels all work in the controller.
        /// </summary>
        public void CancelWork()
        {
            // CancellationTokenSource.Cancel returns only if all registered callbacks are executed.
            // Thus, this method won't return immediately if there are many outstanding tasks to be cancelled
            // within this controller.
            // Trigger the CancellationTokenSource asynchronously. Otherwise, all controllers sharing the same
            // userCancellationToken will keep running until this.cancellationTokenSource.Cancel() returns.
            Task.Run(() =>
                {
                    lock (this.cancelLock)
                    {
                        if (this.cancellationTokenSource != null)
                        {
                            this.cancellationTokenSource.Cancel();
                        }
                    }
                });
        }

        /// <summary>
        /// Public dispose method to release all resources owned.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void CheckCancellation()
        {
            Utils.CheckCancellation(this.cancellationTokenSource.Token);
        }

        public void UpdateProgress(Action updateAction)
        {
            try
            {
                this.TransferJob.ProgressUpdateLock?.EnterReadLock();
                updateAction();
            }
            finally
            {
                this.TransferJob.ProgressUpdateLock?.ExitReadLock();
            }

        }

        public void UpdateProgressAddBytesTransferred(long bytesTransferredToAdd)
        {
            this.TransferJob.Transfer.ProgressTracker.AddBytesTransferred(bytesTransferredToAdd);
        }

        public void StartCallbackHandler()
        {
            if (this.TransferJob.Status == TransferJobStatus.NotStarted)
            {
                this.TransferJob.Status = TransferJobStatus.Transfer;
            }
        }

        public void FinishCallbackHandler(Exception exception)
        {
            if (Interlocked.CompareExchange(ref this.notifiedFinish, 1, 0) == 0)
            {
                ThreadPool.QueueUserWorkItem((userData) =>
                {
                    if (null != exception)
                    {
                        this.TaskCompletionSource.SetException(exception);
                    }
                    else
                    {
                        this.TaskCompletionSource.SetResult(null);
                    }
                });
            }
        }

        protected abstract Task<bool> DoWorkInternalAsync();

        /// <summary>
        /// Pre work action.
        /// </summary>
        protected void PreWork()
        {
            Interlocked.Increment(ref this.activeTasks);
        }

        /// <summary>
        /// Post work action.
        /// </summary>
        /// <returns>
        /// Count of current active task in the controller.
        /// A Controller can only be destroyed after this count of active tasks is 0.
        /// </returns>
        protected bool PostWork()
        {
            lock (this.lockOnFinished)
            {
                return 0 == Interlocked.Decrement(ref this.activeTasks) && this.isFinished;
            }
        }

        protected bool SetFinishedAndPostWork()
        {
            lock (this.lockOnFinished)
            {
                this.isFinished = true;
                return 0 == Interlocked.Decrement(ref this.activeTasks) && this.isFinished;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                try
                {
                    this.transferSchedulerCancellationTokenRegistration.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // Object has been disposed before, just catch this exception, do nothing else.
                }

                try
                {
                    this.userCancellationTokenRegistration.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // Object has been disposed before, just catch this exception, do nothing else.
                }

                try
                {
                    lock (this.cancelLock)
                    {
                        this.cancellationTokenSource.Dispose();
                        this.cancellationTokenSource = null;
                    }
                }
                catch (ObjectDisposedException)
                {
                    // Object has been disposed before, just catch this exception, do nothing else.
                }
            }
        }

        /// <summary>
        /// Sets the state of the controller to Error, while recording
        /// the last occurred exception and setting the HasWork and 
        /// IsFinished fields.
        /// </summary>
        /// <param name="ex">Exception to record.</param>
        protected abstract void SetErrorState(Exception ex);

        public async Task CheckOverwriteAsync(
            bool exist,
            object source,
            object dest)
        {
            if (null == this.TransferJob.Overwrite)
            {
                if (exist)
                {
                    if (null == this.TransferContext || null == this.TransferContext.ShouldOverwriteCallbackAsync || !await this.TransferContext.ShouldOverwriteCallbackAsync(source, dest))
                    {
                        this.TransferJob.Overwrite = false;
                    }
                    else
                    {
                        this.TransferJob.Overwrite = true;
                    }
                }
                else
                {
                    this.TransferJob.Overwrite = true;
                }

                this.TransferJob.Transfer.UpdateJournal();
            }

            if (exist && !this.TransferJob.Overwrite.Value)
            {
                string exceptionMessage = string.Format(CultureInfo.InvariantCulture, Resources.OverwriteCallbackCancelTransferException, source.ConvertToString(), dest.ConvertToString());
                throw new TransferSkippedException(exceptionMessage);
            }
        }

        public async Task SetCustomAttributesAsync(object source, object dest)
        {
            if (null != this.TransferContext && null != this.TransferContext.SetAttributesCallbackAsync)
            {
                await this.TransferContext.SetAttributesCallbackAsync(source, dest);
            }
        }
    }
}
