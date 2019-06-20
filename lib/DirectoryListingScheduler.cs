//------------------------------------------------------------------------------
// <copyright file="DirectoryListingScheduler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    class DirectoryListingScheduler
    {
        SemaphoreSlim semaphore = null;
        private static DirectoryListingScheduler SchedulerInstance = null;
        private static object SingletonLock = new object();

        private DirectoryListingScheduler()
        {
            int parallelLevel = TransferManager.Configurations.ParallelOperations;
            parallelLevel = 2; // (int)Math.Ceiling(((double)parallelLevel) / 8);
            semaphore = new SemaphoreSlim(parallelLevel, parallelLevel);
        }

        public static DirectoryListingScheduler Instance()
        {
            if (null == SchedulerInstance)
            {
                lock (SingletonLock)
                {
                    if (null == SchedulerInstance)
                    {
                        SchedulerInstance = new DirectoryListingScheduler();
                    }
                }
            }

            return SchedulerInstance;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public Task Schedule(
            SubDirectoryTransfer directoryTransfer,
            CancellationToken cancellationToken,
            Action persistDirTransfer,
            int timeOut)
        {
            if (this.semaphore.Wait(timeOut, cancellationToken))
            {
                try
                {
                    if (null != persistDirTransfer)
                    {
                        persistDirTransfer();
                    }
                }
                catch
                {
                    this.semaphore.Release();
                    throw;
                }

                Task task = directoryTransfer.ExecuteAsync(cancellationToken);
                task.ContinueWith((sourceTask) =>
                {
                    this.semaphore.Release();
                    return sourceTask;
                });

                return task;
            }
            else
            {
                return null;
            }
        }
    }
}
