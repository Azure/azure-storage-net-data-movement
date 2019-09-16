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

    class DirectoryListingScheduler : IDisposable
    {
        // Value to control concurrent of directory listing.
        // By testing result with downloading from Azure File Directory on 16 core machines on .Net on Windows and on .Net Core on Linux
        // it has best downloading speed with 2 listing threads on .Net on Windows and with 4 listing threads on .Net Core on Linux.
#if DOTNET5_4
        private const int MaxParallelListingThreads = 6;
#else
        private const int MaxParallelListingThreads = 3;
#endif

        SemaphoreSlim semaphore = null;
        private static DirectoryListingScheduler SchedulerInstance = null;
        private static object SingletonLock = new object();

        private DirectoryListingScheduler()
        {
            semaphore = new SemaphoreSlim(MaxParallelListingThreads, MaxParallelListingThreads);
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
            SubDirectoryTransfer subDirectoryTransfer,
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

                Task task = subDirectoryTransfer.ExecuteAsync(cancellationToken);
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

        /// <summary>
        /// Public dispose method to release all resources owned.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.semaphore)
                {
                    this.semaphore.Dispose();
                    this.semaphore = null;
                }
            }
        }
    }
}
