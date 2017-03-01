//------------------------------------------------------------------------------
// <copyright file="LongPathFileStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Inter calss to support long path files.
    /// </summary>
    internal class LongPathFileStream : Stream
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
            if (offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count), "ArgumentOutOfRange_NeedNonNegNum");
            if (!CanRead)
                throw new NotSupportedException("NotSupported_UnreadableStream");

            uint read = 0;
#if !DOTNET5_4
            NativeOverlapped template = new NativeOverlapped();
            template.EventHandle = IntPtr.Zero;
            template.OffsetLow = (int)(Position & uint.MaxValue);
            template.OffsetHigh = (int)(Position >> 32);
            if (offset != 0)
            {
                var tempBuffer = new byte[count];
                FileNativeMethods.ReadFile(this.fileHandle, tempBuffer, (uint)(count), out read, ref template);
                tempBuffer.CopyTo(buffer, offset);
            }
            else
            {
                FileNativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, ref template);
            }
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

        public override long Seek(long offset, SeekOrigin seekOrigin)
        {
            if (!CanSeek)
            {
                throw new InvalidOperationException("Current long path file stream is not able to seek.");
            }

            Position = FileNativeMethods.Seek(this.fileHandle, offset, seekOrigin);
            return Position;
        }

        public override void SetLength(long value)
        {
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
            if (offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count), "ArgumentOutOfRange_NeedNonNegNum");
            if (!CanWrite)
                throw new NotSupportedException("NotSupported_UnwritableStream");

            uint written = 0;
#if !DOTNET5_4
            if(offset != 0)
            {
                buffer = buffer.Skip(offset).ToArray();
            }
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
            string ret = LongPath.GetFullPath(localFilePath);
            if (ret.StartsWith(@"\\", StringComparison.Ordinal))
            {
                return ret;
            }
            return @"\\?\" + ret;
        }
    }

    internal class LongPath
    {
        private LongPath() { }

        public static string GetFullPath(string path)
        {
#if DOTNET5_4
            return Path.GetFullPath(path);
#else
            int buffSize = 260;
            StringBuilder fullPath = new StringBuilder(buffSize);
            StringBuilder fileName = new StringBuilder(buffSize);
            uint actualSize = FileNativeMethods.GetFullPathNameW(path, (uint)buffSize, fullPath, fileName);
            if (actualSize > buffSize)
            {
                buffSize = (int)actualSize + 16;
                fullPath = new StringBuilder(buffSize);
                fileName = new StringBuilder(buffSize);
                actualSize = FileNativeMethods.GetFullPathNameW(path, (uint)buffSize, fullPath, fileName);
            }

            return fullPath.ToString();
#endif
        }

        public static string Combine(string path1, string path2)
        {
#if DOTNET5_4
            return Path.Combine(path1, path2);
#else
            return Path.Combine(path1, path2);
#endif
        }

        public static string GetDirectoryName(string path)
        {
#if DOTNET5_4
            return Path.GetDirectoryName(path);
#else
            return Path.GetDirectoryName(path);
#endif
        }

        public static string GetFileName(string path)
        {
#if DOTNET5_4
            return Path.GetFileName(path);
#else
            return Path.GetFileName(path);
#endif
        }
    }

    internal class LongPathDirectory
    {
        private LongPathDirectory() { }

        public static bool Exists(string path)
        {
#if DOTNET5_4
            return Directory.Exists(path);
#else
            return Directory.Exists(path);
#endif
        }

#if !CODE_ACCESS_SECURITY
        public static string[] GetFiles(string path)
        {
#if DOTNET5_4
            return Directory.GetFiles(path);
#else
            return Directory.GetFiles(path);
#endif
        }
#endif

        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption)
        {
#if DOTNET5_4
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#endif
        }
    }

    internal class LongPathFile
    {
        private LongPathFile() { }

        public static FileAttributes GetAttributes(string path)
        {
#if DOTNET5_4
            return File.GetAttributes(path);
#else
            return File.GetAttributes(path);
#endif
        }
    }
}
