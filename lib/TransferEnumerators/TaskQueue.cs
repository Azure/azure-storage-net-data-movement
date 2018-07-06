//------------------------------------------------------------------------------
// <copyright file="TaskQueue.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Task scheduler that schedule the tasks concorrently and FIFO return the result.
    /// </summary>
    /// <typeparam name="T">Return type of the scheduled tasks.</typeparam>
    internal class TaskQueue<T> : IDisposable
    {
        private BlockingCollection<Task<T>> queue;

        public TaskQueue(int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentException("Capacity must be a positive integer.", "capacity");
            }

            queue = new BlockingCollection<Task<T>>(capacity);
        }

        /// <summary>
        /// Enqueue a task into the tasks queue. This method blocks if the queue reaches its capacity.
        /// </summary>
        /// <param name="func">Task to enqueue.</param>
        public void EnqueueJob(Func<Task<T>> func)
        {
            queue.Add(Task.Run(func));
        }

        /// <summary>
        /// Dequeue the result returned by the enqueued task. This method blocks if the queue is empty.
        /// </summary>
        /// <returns>Result from enqueued task.</returns>
        public T DequeueResult()
        {
            return queue.Take().Result;
        }

        /// <summary>
        /// Set this queue to CompleteAdding. Call to DequeueResult throws InvalidOperationException afterwards.
        /// </summary>
        public void CompleteAdding()
        {
            this.queue.CompleteAdding();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.queue.Dispose();
                    this.queue = null;
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
