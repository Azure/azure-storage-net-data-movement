//------------------------------------------------------------------------------
// <copyright file="PipelinedMemoryStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Collections.Generic;
    using System.Threading;

    // The following class is designed under the assumption that writes much are smaller on average than the buffer-size
    // It is still correct without this assumption, but it's performance could be improved
    // This is almost certainly a fair assumption for buffer-sizes >= 4 MB (the default)

    internal class PipelineMemoryStream : Stream
    {
        private byte[] buffer;
        private int length;      // including data which has been returned
        private int offset;      // within the private buffer

        private MemoryManager manager;
        private Action<byte[], int, int> callback;

        // How long to spend trying to allocate memory before giving up
        // In milliseconds
        public int AllocationTimeout { get; set; } = 30 * 1000; // Default 30 seconds

        public PipelineMemoryStream(MemoryManager manager, Action<byte[], int, int> callback)
        {
            this.manager = manager;
            this.callback = callback;
            ReplaceBuffer();
            this.length = buffer.Length;
        }

        // Replace the current buffer with a new one
        private void ReplaceBuffer()
        {
            // On failure, use exponetial backoff
            int sleepTime = 1;
            int remaining = AllocationTimeout;
            do {
                this.buffer = this.manager.RequireBuffer();
                if (this.buffer == null)
                {
                    if (remaining > 0)
                    {
                        Thread.Sleep((int)Math.Min(sleepTime, remaining));
                        remaining -= sleepTime;
                        sleepTime *= 2;
                    }
                    else
                    {
                        // TODO: This doesn't seem to exit the transfer so much as make it get stuck :(
                        throw new TransferException(TransferErrorCode.FailToAllocateMemory);
                    }
                }
            } while (this.buffer == null);
        }

        public override void Flush()
        {
            if (this.offset > 0) {
                // After this call, the receiver owns the sent buffer
                this.callback(this.buffer, 0, this.offset);
                this.length += this.offset;
                this.offset = 0;

                ReplaceBuffer();
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (offset < 0 || count < 0 || buffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }


            int remaining = count;
            int availible = this.buffer.Length - this.offset;

            while (remaining > 0)
            {
                // Find n: The bytes to be written on this iteration
                int n = remaining;
                if (n > availible) {
                    n = availible;
                }

                //Write n bytes to the current chunk
                Array.Copy(buffer, offset, this.buffer, this.offset, n);
                offset += n;
                this.offset += n;
                remaining -= n;

                // If we have just filled our internal buffer, flush
                availible = this.buffer.Length - this.offset;
                if (availible == 0) {
                    this.Flush();
                    availible = this.buffer.Length;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.manager.ReleaseBuffer(this.buffer);
                this.buffer = null;
            }

            base.Dispose(disposing);
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return this.length; }
        }

        public override long Position
        {
            get { return this.length - this.buffer.Length + this.offset; }
            set { throw new NotSupportedException(); }
        }

        // Unsupported methods
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
