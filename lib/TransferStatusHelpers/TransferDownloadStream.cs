//------------------------------------------------------------------------------
// <copyright file="TransferDownloadStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    class TransferDownloadStream : Stream
    {
        TransferDownloadBuffer firstBuffer;
        Stream firstStream;
        int firstOffset;

        TransferDownloadBuffer secondBuffer;
        Stream secondStream;
        int secondOffset;

        bool onSecondStream = false;

        MemoryManager memoryManager;

        public TransferDownloadStream(MemoryManager memoryManager, TransferDownloadBuffer buffer, int offset, int count)
            :this(memoryManager, buffer, offset, count, null, 0, 0)
        {
        }

        public TransferDownloadStream(
            MemoryManager memoryManager, 
            TransferDownloadBuffer firstBuffer, 
            int firstOffset, 
            int firstCount,
            TransferDownloadBuffer secondBuffer,
            int secondOffset,
            int secondCount)
        {
            this.memoryManager = memoryManager;
            this.firstBuffer = firstBuffer;
            this.firstOffset = firstOffset;

            if (firstBuffer.MemoryBuffer.Length == 1)
            {
                this.firstStream = new MemoryStream(this.firstBuffer.MemoryBuffer[0], firstOffset, firstCount);
            }
            else
            {
                this.firstStream = new ChunkedMemoryStream(this.firstBuffer.MemoryBuffer, firstOffset, firstCount);
            }

            if (null != secondBuffer)
            {
                this.secondBuffer = secondBuffer;
                this.secondOffset = secondOffset;

                if (secondBuffer.MemoryBuffer.Length == 1)
                {
                    this.secondStream = new MemoryStream(this.secondBuffer.MemoryBuffer[0], secondOffset, secondCount);
                }
                else
                {
                    this.secondStream = new ChunkedMemoryStream(this.secondBuffer.MemoryBuffer, secondOffset, secondCount);
                }
            }
        }

        public override bool CanRead
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public bool ReserveBuffer
        {
            get;
            set;
        }

        public override long Length
        {
            get
            {
                if (null == this.secondStream)
                {
                    return this.firstStream.Length;
                }

                return this.firstStream.Length + this.secondStream.Length;
            }
        }

        public override long Position
        {
            get
            {
                if (!this.onSecondStream)
                {
                    return this.firstStream.Position;
                }
                else
                {
                    Debug.Assert(null != this.secondStream, "Second stream should exist when position is on the second stream");
                    return this.firstStream.Length + this.secondStream.Position;
                }
            }

            set
            {
                long position = value;

                if (position < this.firstStream.Length)
                {
                    this.onSecondStream = false;
                    this.firstStream.Position = position;
                }
                else
                {
                    position -= this.firstStream.Length;
                    this.onSecondStream = true;
                    this.secondStream.Position = position;
                }
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long position = 0;

            switch (origin)
            {
                case SeekOrigin.End:
                    position = this.Length + offset;
                    break;
                case SeekOrigin.Current:
                    position = this.Position + offset;
                    break;
                default:
                    position = offset;
                    break;
            }

            this.Position = position;
            return position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            // do nothing
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            int length = count;
            int firstLength = 0;
            if (!this.onSecondStream)
            {
                firstLength = Math.Min(length, (int)(this.firstStream.Length - this.firstStream.Position));
                this.firstStream.Write(buffer, offset, firstLength);
                length -= firstLength;
                if (0 == length)
                {
                    return;
                }
                else
                {
                    if (null == this.secondStream)
                    {
                        throw new NotSupportedException(Resources.StreamNotExpandable);
                    }

                    this.onSecondStream = true;
                }
            }

            Debug.Assert(null != this.secondStream, "Position is on the second stream, it should not be null");

            this.secondStream.Write(buffer, offset + firstLength, length);
        }

        public void SetAllZero()
        {
            if (this.firstBuffer.MemoryBuffer.Length == 1)
            {
                Array.Clear(this.firstBuffer.MemoryBuffer[0], this.firstOffset, (int) this.firstStream.Length);
            }
            else
            {
                SetAllZero(this.firstBuffer.MemoryBuffer, this.firstOffset, (int)this.firstStream.Length);
            }

            if (null != this.secondBuffer)
            {
                if (this.secondBuffer.MemoryBuffer.Length == 1)
                {
                    Array.Clear(this.secondBuffer.MemoryBuffer[0], this.secondOffset, (int) this.secondStream.Length);
                }
                else
                {
                    SetAllZero(this.secondBuffer.MemoryBuffer, this.secondOffset, (int)this.secondStream.Length);
                }
            }
        }

        private static void SetAllZero(byte[][] buffers, int offset, int length)
        {
            //TODO: Duplicate code
            var currentChunk = 0;
            var currentChunkOffset = 0;

            // Seek to the correct chunk and offset
            while (offset != 0 && currentChunk != buffers.Length)
            {
                if (buffers[currentChunk].Length > offset)
                {
                    // Found the correct chunk and it's offset
                    currentChunkOffset = offset;
                    break;
                }

                // Move to next chunk
                offset -= buffers[currentChunk].Length;
                currentChunk += 1;
                currentChunkOffset = 0;
            }

            while (length != 0 && currentChunk != buffers.Length)
            {
                var remainingCountInCurrentChunk = buffers[currentChunk].Length - currentChunkOffset;
                var bytesToClear = Math.Min(remainingCountInCurrentChunk, length);

                Array.Clear(buffers[currentChunk], currentChunkOffset, bytesToClear);

                if (remainingCountInCurrentChunk <= length)
                {
                    // Move to next chunk
                    currentChunk++;
                    currentChunkOffset = 0;
                }

                length -= bytesToClear;
            }
        }

        public void FinishWrite()
        {
            this.firstBuffer.ReadFinish((int)this.firstStream.Length);

            if (null != this.secondBuffer)
            {
                this.secondBuffer.ReadFinish((int)this.secondStream.Length);
            }
        }

        public IEnumerable<TransferDownloadBuffer> GetBuffers()
        {
            yield return this.firstBuffer;

            if (null != this.secondBuffer)
            {
                yield return this.secondBuffer;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {

                if (null != this.firstStream)
                {
                    this.firstStream.Dispose();
                    this.firstStream = null;
                }

                if (null != this.secondStream)
                {
                    this.secondStream.Dispose();
                    this.secondStream = null;
                }

                if (!this.ReserveBuffer)
                {
                    if (null != this.firstBuffer)
                    {
                        this.memoryManager.ReleaseBuffers(this.firstBuffer.MemoryBuffer);
                        this.firstBuffer = null;
                    }

                    if (null != this.secondBuffer)
                    {
                        this.memoryManager.ReleaseBuffers(this.secondBuffer.MemoryBuffer);
                        this.secondBuffer = null;
                    }
                }
            }
        }
    }
}
