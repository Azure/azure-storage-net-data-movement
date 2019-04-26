//------------------------------------------------------------------------------
// <copyright file="ChunkedMemoryStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;

#if DEBUG
    /// <summary>
    /// MemoryStream with chunked underlying toBuffer. Only support user-provided toBuffer and does NOT allow user to adjust the stream length.
    /// </summary>
    public class ChunkedMemoryStream : Stream
#else
    internal class ChunkedMemoryStream : Stream
#endif
    {
        private readonly byte[][] buffer;
        private readonly int origin;
        private readonly int length;

        private int position;
        private int currentChunk;
        private int currentChunkOffset;

        public ChunkedMemoryStream(byte[][] buffer, int index, int count)
        {
            this.buffer = buffer;
            this.length = index + count;
            this.origin = index;

            this.SetPosition(0);
        }

        public override void Flush()
        {
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1725:ParameterNamesShouldMatchBaseDeclaration", MessageId = "1#")]
        public override long Seek(long offset, SeekOrigin seekOrigin)
        {
            var newPos = 0;
            switch (seekOrigin)
            {
                case SeekOrigin.Begin:
                    newPos = (int)offset;
                    break;

                case SeekOrigin.Current:
                    newPos = (int) (this.Position + offset);
                    break;

                case SeekOrigin.End:
                    newPos = (int) (this.Length + offset);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(offset));
            }

            this.Position = newPos;
            return this.position;
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1725:ParameterNamesShouldMatchBaseDeclaration", MessageId = "0#")]
        public override int Read(byte[] toBuffer, int offset, int count)
        {
            if (toBuffer == null)
            {
               throw new ArgumentNullException(nameof(toBuffer)); 
            }

            if(offset < 0 || count < 0 || toBuffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            var bytesReaded = 0;
            var bytesToRead = this.length - this.position;
            if (bytesToRead > count)
            {
                bytesToRead = count;
            }
            if (bytesToRead <= 0)
            {
                // Already at the end of the stream, nothing to read
                return 0;
            }

            while (bytesToRead != 0 && this.currentChunk != this.buffer.Length)
            {
                var moveToNextChunk = false;
                var chunk = this.buffer[this.currentChunk];
                var n = bytesToRead;
                var remainingBytesInCurrentChunk = chunk.Length - this.currentChunkOffset;
                if (n >= remainingBytesInCurrentChunk)
                {
                    n = remainingBytesInCurrentChunk;
                    moveToNextChunk = true;
                }

                //Read n bytes from the current chunk
                Array.Copy(chunk, this.currentChunkOffset, toBuffer, offset, n);
                bytesToRead -= n;
                offset += n;
                bytesReaded += n;

                if (moveToNextChunk)
                {
                    this.currentChunkOffset = 0;
                    this.currentChunk++;
                }
                else
                {
                    this.currentChunkOffset += n;
                }
            }

            this.position += bytesReaded;
            return bytesReaded;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1725:ParameterNamesShouldMatchBaseDeclaration", MessageId = "0#")]
        public override void Write(byte[] fromBuffer, int offset, int count)
        {
            if (fromBuffer == null)
            {
                throw new ArgumentNullException(nameof(fromBuffer));
            }

            if (offset < 0 || count < 0 || fromBuffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }


            var bytesWritten = 0;
            var bytesToWrite = this.length - this.position;
            if (bytesToWrite > count)
            {
                bytesToWrite = count;
            }

            if (bytesToWrite <= 0)
            {
                // Already at the end of the stream, nothing to write
                return;
            }


            while (bytesToWrite != 0 && this.currentChunk != this.buffer.Length)
            {
                var moveToNextChunk = false;
                var chunk = this.buffer[this.currentChunk];
                var n = bytesToWrite;
                var remainingBytesInCurrentChunk = chunk.Length - this.currentChunkOffset;
                if (n >= remainingBytesInCurrentChunk)
                {
                    n = remainingBytesInCurrentChunk;
                    moveToNextChunk = true;
                }

                //Write n bytes to the current chunk
                Array.Copy(fromBuffer, offset, chunk, this.currentChunkOffset, n);
                bytesToWrite -= n;
                offset += n;
                bytesWritten += n;

                if (moveToNextChunk)
                {
                    this.currentChunkOffset = 0;
                    this.currentChunk++;
                }
                else
                {
                    this.currentChunkOffset += n;
                }
            }

            this.position += bytesWritten;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return this.length - this.origin; }
        }

        public override long Position
        {
            get { return this.position - this.origin; }
            set { SetPosition(value); }
        }

        private void SetPosition(long value)
        {
            var newPosition = this.origin + (int) value;
            if (newPosition < this.origin || newPosition >= this.length)
            {
                throw new ArgumentOutOfRangeException(nameof(value));
            }

            this.position = newPosition;

            // Find the current chunk & current chunk offset
            var currentChunkIndex = 0;
            var offset = newPosition;

            while (offset != 0)
            {
                var chunkLength = this.buffer[currentChunkIndex].Length;
                if (offset < chunkLength)
                {
                    // Found the correct chunk and the corresponding offset
                    break;
                }

                offset -= chunkLength;
                currentChunkIndex++;
            }

            this.currentChunk = currentChunkIndex;
            this.currentChunkOffset = offset;
        }
    }
}
