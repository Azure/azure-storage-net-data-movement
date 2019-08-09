

namespace DMLibTest.Util
{
    using System;
    using System.IO;

    class DMLibTestStream : Stream
    {
        FileStream internalStream = null;
        bool seekable;
        bool fixedSize;

        public DMLibTestStream(FileStream stream, bool seekable = true, bool fixedSize = true)
        {
            this.internalStream = stream;
            this.seekable = seekable;
            this.fixedSize = fixedSize;
        }

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => this.seekable;
        public override long Position
        {
            get
            {
                if (this.seekable)
                {
                    return this.internalStream.Position;
                }

                throw new NotSupportedException();
            }

            set
            {
                if (this.seekable)
                {
                    this.internalStream.Position = value;
                }

                throw new NotSupportedException();
            }
        }

        public override long Length
        {
            get
            {
                if (fixedSize)
                {
                    return this.internalStream.Length;
                }

                throw new NotSupportedException();
            }
        }

        public override void Flush()
        {
            this.internalStream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (this.seekable)
            {
                return this.internalStream.Seek(offset, origin);
            }

            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return this.internalStream.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.internalStream.Write(buffer, offset, count);
        }

        public override void Close()
        {
            base.Close();
            this.internalStream.Close();
        }
    }
}
