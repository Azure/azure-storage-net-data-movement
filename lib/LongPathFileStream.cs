using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    public class LongPathFileStream : Stream
    {
        private long position = 0;
        private string filePath = null;
        private SafeFileHandle fileHandle = null;

        public LongPathFileStream(string filePath, FileMode mode, FileAccess access, FileShare share)
        {
            this.filePath = ToUNCPath(filePath);
            this.fileHandle = FileNativeMethods.CreateFile(this.filePath,
                access,
                share,
                IntPtr.Zero,
                mode,
                FileAttributes.Normal,
                IntPtr.Zero);

            if (this.fileHandle.IsInvalid)
            {
                // 183 means the file already exists, while open succeeded.
                int errorCode = Marshal.GetLastWin32Error();
                if ((0 != errorCode)
                    && (183 != errorCode))
                {
                    System.Console.WriteLine(this.filePath);
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
        }
        /*
        ~LongPathFileStream()
        {
#if DOTNET5_4
            this.Dispose(false);
#else
            this.Close();
#endif
        }
        */

#if DOTNET5_4
        public new void Dispose()
        {
            this.Dispose(true);
        }
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
#else
        public override void Close()
        {
#endif
            if (this.fileHandle != null && !this.fileHandle.IsClosed)
            {
#if DOTNET5_4
                this.fileHandle.Dispose();
#else
                this.fileHandle.Close();
#endif
                // FileNativeMethods.CloseHandle(this.fileHandle);
                this.fileHandle = null;

                if (this.filePath != null)
                {
                    this.filePath = null;
                }

                if (this.position != 0)
                {
                    this.position = 0;
                }
            }
        }

        public override bool CanRead
        {
            get
            {
                if (this.fileHandle != null && !this.fileHandle.IsClosed)
                {
                    return !this.fileHandle.IsInvalid;
                }
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                if (this.fileHandle != null && !this.fileHandle.IsClosed)
                {
                    return !this.fileHandle.IsInvalid;
                }
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                if(this.fileHandle != null && !this.fileHandle.IsClosed)
                {
                    return !this.fileHandle.IsInvalid;
                }
                return false;
            }
        }

        public override long Length
        {
            get
            {
                if (this.fileHandle != null && !this.fileHandle.IsClosed)
                {
                    return FileNativeMethods.GetFileSize(this.fileHandle, IntPtr.Zero);
                }
                else
                {
                    throw new NotSupportedException("null or closed");
                }
            }
        }

        public override long Position
        {
            get
            {
                return this.position;
            }

            set
            {
                if(!CanSeek)
                {
                    throw new NotSupportedException("Not able to seek");
                }
                if(this.position != value)
                {
                    this.position = value;
                }
                this.position = value;
            }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "ArgumentOutOfRange_NeedNonNegNum");
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count), "ArgumentOutOfRange_NeedNonNegNum");


            if (!CanRead)
                throw new NotSupportedException("NotSupported_UnreadableStream");

            uint read = 0;
#if !DOTNET5_4
            NativeOverlapped template = new NativeOverlapped();
            template.EventHandle = IntPtr.Zero;
            template.OffsetLow = (int)(Position & uint.MaxValue);
            template.OffsetHigh = (int)(Position >> 32);
            FileNativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, ref template);
#else
            FileNativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, IntPtr.Zero);
#endif

            int errorCode = Marshal.GetLastWin32Error();

            if ((0 != errorCode)
                && (183 != errorCode))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            Position += read;
            return (int)(read);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if(CanSeek)
            {
                this.position = FileNativeMethods.Seek(this.fileHandle, offset, SeekOrigin.Begin);
            }
            //if(origin != SeekOrigin.Begin || offset != this.position)
            //{
            //}
            return this.position;
        }

        public override void SetLength(long value)
        {
            // check
            Seek(value, SeekOrigin.Begin);
            FileNativeMethods.SetEndOfFile(this.fileHandle);
            Seek(this.position, SeekOrigin.Begin);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "ArgumentOutOfRange_NeedNonNegNum");
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count), "ArgumentOutOfRange_NeedNonNegNum");
            if (!CanWrite)
                throw new NotSupportedException("NotSupported_UnwritableStream");

            uint written = 0;
#if !DOTNET5_4
            NativeOverlapped template = new NativeOverlapped();
            template.EventHandle = IntPtr.Zero;
            template.OffsetLow = (int)(Position & uint.MaxValue);
            template.OffsetHigh = (int)(Position >> 32);
            FileNativeMethods.WriteFile(this.fileHandle, buffer, (uint)(count), out written, ref template);
#else
            FileNativeMethods.WriteFile(this.fileHandle, buffer, (uint)(count), out written, IntPtr.Zero);
#endif
            int errorCode = Marshal.GetLastWin32Error();
            if ((0 != errorCode)
                && (183 != errorCode))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            Position += written;
        }

        private static string ToUNCPath(string localFilePath)
        {
            string ret = @"\\?\";
            if (localFilePath.StartsWith(@"\\", StringComparison.Ordinal))
            {
                return localFilePath;
            }
            else if (localFilePath.StartsWith(@".", StringComparison.Ordinal))
            {
                return AppendPath(AppendPath(ret, Directory.GetCurrentDirectory()), localFilePath);
            }
            else if (localFilePath.StartsWith(@"\", StringComparison.Ordinal))
            {
                return AppendPath(AppendPath(ret, Directory.GetDirectoryRoot(Directory.GetCurrentDirectory())), localFilePath);
            }
            else
            {
                string[] pieces = localFilePath.Split(':');
                if (pieces.Length == 1)
                {
                    // current folder
                    return AppendPath(AppendPath(ret, Directory.GetCurrentDirectory()), localFilePath);
                }
                if (pieces.Length != 2)
                    throw new ArgumentException("Invalid file path.");
                if (pieces[1].StartsWith(@"\", StringComparison.Ordinal))
                {
                    ret = AppendPath(ret, localFilePath);
                }
                else
                {
                    throw new ArgumentException("Not supported related file path.");
                }
                return ret;
            }
        }

        private static string AppendPath(string a, string b)
        {
            if (a.EndsWith(@"\", StringComparison.Ordinal))
            {
                if (b.StartsWith(@"\", StringComparison.Ordinal))
                {
                    return a + b.Substring(1);
                }
                else
                {
                    return a + b;
                }
            }
            else if (b.StartsWith(@"\", StringComparison.Ordinal))
            {
                return a + b;
            }
            else
            {
                return a + @"\" + b;
            }
        }
    }
}
