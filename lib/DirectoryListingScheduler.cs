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
        SemaphoreSlim semaphore = null;

        public DirectoryListingScheduler(int maxParallelListingThreads)
        {
            semaphore = new SemaphoreSlim(maxParallelListingThreads, maxParallelListingThreads);
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
