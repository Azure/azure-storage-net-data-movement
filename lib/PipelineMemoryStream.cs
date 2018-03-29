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
        private byte[][] buffers = null;
        private int index = 0;    // of the current buffer
        private int length = 0;   // including data which has been returned
        private int offset = 0;   // within current private buffer
        private int position = 0; // across all buffers

        private MemoryManager manager; // Used to return unused buffers
        private Action<byte[], int, int> callback;

        public PipelineMemoryStream(byte[][] buffers, MemoryManager manager, Action<byte[], int, int> callback)
        {
            this.buffers = buffers;
            this.manager = manager;
            this.callback = callback;

            foreach (var buffer in buffers)
            {
                this.length += buffer.Length;
            }
        }

        public override void Flush()
        {
            if (this.offset > 0) {
                // After this call, the receiver owns the sent buffer
                this.callback(this.buffers[this.index], 0, this.offset);

                // Move position forward by the remaining buffer length
                // Any unsued space in the sent buffer is lost to us
                this.position += this.buffers[this.index].Length - this.offset;
                this.buffers[this.index] = null;
                this.offset = 0;
                this.index += 1;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            // Check if we can fit this data in the remaining buffers
            if (this.index >= this.buffers.Length || count > (this.length - this.position))
            {
                // TODO: Add an error message
                throw new InvalidOperationException();
            }

            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (offset < 0 || count < 0 || buffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            int remaining = count;

            while (remaining > 0)
            {
                int availible = this.buffers[this.index].Length - this.offset;
                if (availible <= 0) {
                    this.Flush();
                    availible = this.buffers[this.index].Length;
                }

                // Find n: The bytes to be written on this iteration
                int n = remaining > availible ? availible : remaining;

                //Write n bytes to the current chunk
                Array.Copy(buffer, offset, this.buffers[this.index], this.offset, n);
                offset += n;
                this.offset += n;
                this.position += n;
                remaining -= n;
            }
        }

        protected override void Dispose(bool disposing)
        {

            if (this.buffers != null && this.manager != null)
            {
                for (int i=0; i< this.buffers.Length; i++)
                {
                    if (this.buffers[i] != null)
                    {
                        this.manager.ReleaseBuffer(this.buffers[i]);
                        this.buffers[i] = null;
                    }
                }
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
            get { return this.position; }
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
