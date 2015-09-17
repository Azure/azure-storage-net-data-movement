//------------------------------------------------------------------------------
// <copyright file="TransferDownloadStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    class TransferDownloadStream : Stream
    {
        TransferDownloadBuffer firstBuffer;
        MemoryStream firstStream;
        int firstOffset;

        TransferDownloadBuffer secondBuffer;
        MemoryStream secondStream;
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
            this.firstStream = new MemoryStream(this.firstBuffer.MemoryBuffer, firstOffset, firstCount);

            if (null != secondBuffer)
            {
                this.secondBuffer = secondBuffer;
                this.secondOffset = secondOffset;
                this.secondStream = new MemoryStream(this.secondBuffer.MemoryBuffer, secondOffset, secondCount);
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
            Array.Clear(this.firstBuffer.MemoryBuffer, this.firstOffset, (int)this.firstStream.Length);

            if (null != this.secondBuffer)
            {
                Array.Clear(this.secondBuffer.MemoryBuffer, this.secondOffset, (int)this.secondStream.Length);
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
                        this.memoryManager.ReleaseBuffer(this.firstBuffer.MemoryBuffer);
                        this.firstBuffer = null;
                    }

                    if (null != this.secondBuffer)
                    {
                        this.memoryManager.ReleaseBuffer(this.secondBuffer.MemoryBuffer);
                        this.secondBuffer = null;
                    }
                }
            }
        }
    }
}
