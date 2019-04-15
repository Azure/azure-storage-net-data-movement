//------------------------------------------------------------------------------
// <copyright file="TransferDownloadBuffer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System.Threading;

    class TransferDownloadBuffer
    {
        private int finishedLength = 0;

        private int processed = 0;

        public TransferDownloadBuffer(long startOffset, int expectedLength, byte[][] buffer)
        {
            this.Length = expectedLength;
            this.StartOffset = startOffset;
            this.MemoryBuffer = buffer;
        }

        public int Length
        {
            get;
            private set;
        }

        public long StartOffset
        {
            get;
            private set;
        }

        public byte[][] MemoryBuffer
        {
            get;
            private set;
        }

        public bool Finished
        {
            get
            {
                return this.finishedLength == this.Length;
            }
        }

        /// <summary>
        /// Mark this buffer as processed. The return value indicates whether the buffer
        /// is marked as processed by invocation of this method. This method returns true 
        /// exactly once. The caller is supposed to invoke this method before processing 
        /// the buffer and proceed only if this method returns true.
        /// </summary>
        /// <returns>Whether this instance is marked as processed by invocation of this method.</returns>
        public bool MarkAsProcessed()
        {
            return 0 == Interlocked.CompareExchange(ref this.processed, 1, 0);
        }

        public void ReadFinish(int length)
        {
            Interlocked.Add(ref this.finishedLength, length);
        }
    }
}
