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
    using System.Diagnostics;
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;

    /// <summary>
    /// Inter calss to support long path files.
    /// </summary>
    public class LongPathFileStream : Stream
    {
        private long position = 0;
        private string filePath = null;
        private SafeFileHandle fileHandle = null;

        public LongPathFileStream(string filePath, FileMode mode, FileAccess access, FileShare share)
        {
            this.filePath = LongPath.ToUncPath(filePath);
            this.fileHandle = CreateFile(this.filePath, mode, access, share);
        }

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
                    return NativeMethods.GetFileSize(this.fileHandle, IntPtr.Zero);
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
                NativeMethods.ReadFile(this.fileHandle, tempBuffer, (uint)(count), out read, ref template);
                tempBuffer.CopyTo(buffer, offset);
            }
            else
            {
                NativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, ref template);
            }
#else
            NativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, IntPtr.Zero);
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
            if (!CanSeek)
            {
                throw new InvalidOperationException("Current long path file stream is not able to seek.");
            }

            Position = NativeMethods.Seek(this.fileHandle, offset, origin);
            return Position;
        }

        public override void SetLength(long value)
        {
            Seek(value, SeekOrigin.Begin);
            NativeMethods.SetEndOfFile(this.fileHandle);
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
            NativeMethods.WriteFile(this.fileHandle, buffer, (uint)(count), out written, ref template);
#else
            NativeMethods.WriteFile(this.fileHandle, buffer, (uint)(count), out written, IntPtr.Zero);
#endif
            int errorCode = Marshal.GetLastWin32Error();
            if ((0 != errorCode)
                && (183 != errorCode))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            Position += written;
        }

        private SafeFileHandle CreateFile(string path, FileMode mode, FileAccess access, FileShare share)
        {
            path = LongPath.ToUncPath(path);
            UnicodeEncoding unicode = new UnicodeEncoding();
            var unicodePath = unicode.GetBytes(path);

            this.fileHandle = NativeMethods.CreateFile(unicodePath,
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
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
            return this.fileHandle;
        }
    }

    internal class LongPath
    {
        private LongPath() { }

        public static string ToUncPath(string localFilePath)
        {
            string ret = LongPath.GetFullPath(localFilePath);
            if (ret.StartsWith(@"\\", StringComparison.Ordinal))
            {
                return ret;
            }
            return @"\\?\" + ret;
        }

        public static string GetFullPath(string path)
        {
#if DOTNET5_4
            return Path.GetFullPath(path);
#else
            int buffSize = 260;
            StringBuilder fullPath = new StringBuilder(buffSize);
            StringBuilder fileName = new StringBuilder(buffSize);
            uint actualSize = NativeMethods.GetFullPathNameW(path, (uint)buffSize, fullPath, fileName);
            if (actualSize > buffSize)
            {
                buffSize = (int)actualSize + 16;
                fullPath = new StringBuilder(buffSize);
                fileName = new StringBuilder(buffSize);
                actualSize = NativeMethods.GetFullPathNameW(path, (uint)buffSize, fullPath, fileName);
            }

            return fullPath.ToString();
#endif
        }

        public static string Combine(string path1, string path2)
        {
#if DOTNET5_4
            return Path.Combine(path1, path2);
#else
            if (path1 == null || path2 == null)
                throw new ArgumentNullException((path1 == null) ? nameof(path1) : nameof(path2));
            // checck if the path is invalid

            if (path2.Length == 0)
                return path1;

            if (path1.Length == 0)
                return path2;

            if ((path2.Length >= 1 && path2[0] == Path.DirectorySeparatorChar)
                || (path2.Length >= 2 && path2[1] == Path.VolumeSeparatorChar))
                return path2;

            char lastChar = path1[path1.Length - 1];
            return (lastChar == Path.DirectorySeparatorChar || lastChar == Path.AltDirectorySeparatorChar || lastChar == Path.VolumeSeparatorChar) ?
                path1 + path2 :
                path1 + Path.DirectorySeparatorChar.ToString() + path2;
#endif
        }

        public static string GetDirectoryName(string path)
        {
#if DOTNET5_4
            return Path.GetDirectoryName(path);
#else
            if (path != null)
            {

                int lastSeparator = path.Length;
                while (lastSeparator > 0 && path[--lastSeparator] != Path.DirectorySeparatorChar && path[lastSeparator] != Path.AltDirectorySeparatorChar) ;
                if (lastSeparator > GetRootLength(path))
                {
                    return path.Substring(0, lastSeparator);
                }
            }
            return null;
#endif
        }

        // TODO
        private static int GetRootLength(string path)
        {
            int pathLength = path.Length;
            int i = 0;
            int volumeSeparatorLength = 2;  // Length to the colon "C:"
            int uncRootLength = 2;          // Length to the start of the server name "\\"

            const string ExtendedPathPrefix = @"\\?\";
            const string UncExtendedPathPrefix = @"\\?\UNC\";
            bool extendedSyntax = path.StartsWith(ExtendedPathPrefix, StringComparison.Ordinal);
            bool extendedUncSyntax = path.StartsWith(UncExtendedPathPrefix, StringComparison.Ordinal);
            if (extendedSyntax)
            {
                // Shift the position we look for the root from to account for the extended prefix
                if (extendedUncSyntax)
                {
                    // "\\" -> "\\?\UNC\"
                    uncRootLength = UncExtendedPathPrefix.Length;
                }
                else
                {
                    // "C:" -> "\\?\C:"
                    volumeSeparatorLength += ExtendedPathPrefix.Length;
                }
            }

            if ((!extendedSyntax || extendedUncSyntax) && pathLength > 0 && IsDirectorySeparator(path[0]))
            {
                // UNC or simple rooted path (e.g. "\foo", NOT "\\?\C:\foo")

                i = 1; //  Drive rooted (\foo) is one character
                if (extendedUncSyntax || (pathLength > 1 && IsDirectorySeparator(path[1])))
                {
                    // UNC (\\?\UNC\ or \\), scan past the next two directory separators at most
                    // (e.g. to \\?\UNC\Server\Share or \\Server\Share\)
                    i = uncRootLength;
                    int n = 2; // Maximum separators to skip
                    while (i < pathLength && (!IsDirectorySeparator(path[i]) || --n > 0)) i++;
                }
            }
            else if (pathLength >= volumeSeparatorLength && path[volumeSeparatorLength - 1] == Path.VolumeSeparatorChar)
            {
                // Path is at least longer than where we expect a colon, and has a colon (\\?\A:, A:)
                // If the colon is followed by a directory separator, move past it
                i = volumeSeparatorLength;
                if (pathLength >= volumeSeparatorLength + 1 && IsDirectorySeparator(path[volumeSeparatorLength])) i++;
            }
            return i;
        }

        private static bool IsDirectorySeparator(char ch)
        {
            return ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar;
        }

        public static string GetFileNameWithoutExtension(string path)
        {

#if DOTNET5_4
            return Path.GetFileNameWithoutExtension(path);
#else
            if (path == null)
                return path;

            int length = path.Length;
            int start = FindFileNameIndex(path);

            int end = path.LastIndexOf('.', length - 1, length - start);
            return end == -1 ?
                path.Substring(start) :
                path.Substring(start, end - start);
#endif
        }

        public static string GetFileName(string path)
        {
#if DOTNET5_4
            return Path.GetFileName(path);
#else
            if (path == null)
                return null;

            int offset = FindFileNameIndex(path);
            return path.Substring(offset);
#endif
        }

        private static int FindFileNameIndex(string path)
        {
            Debug.Assert(path != null);

            for(int i = path.Length - 1; i >= 0; --i)
            {
                char ch = path[i];
                if (ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar || ch == Path.VolumeSeparatorChar)
                    return i + 1;
            }

            return 0;
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
            path = LongPath.ToUncPath(path);
            bool ret = NativeMethods.PathFileExists(path);
            int errorCode = Marshal.GetLastWin32Error();

            if (ret)
                return ret;

            if (0 != errorCode
                && NativeMethods.ERROR_FILE_NOT_FOUND != errorCode)
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            return ret;
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

        public static void CreateDirectory(string path)
        {
#if DOTNET5_4
            Directory.CreateDirectory(path);
#else
            path = LongPath.ToUncPath(path);
            UnicodeEncoding unicode = new UnicodeEncoding();
            var unicodeDirectoryPath = unicode.GetBytes(path);

            bool ret = NativeMethods.CreateDirectory(unicodeDirectoryPath, IntPtr.Zero);
            int errorCode = Marshal.GetLastWin32Error();
            if(ret == false)
            {
                if ((0 != errorCode)
                    && (183 != errorCode))
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
#endif
        }

        // only SearchOption.TopDirectoryOnly is supported.
        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption)
        {
#if DOTNET5_4
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
            // check search pattern.
            if (searchOption == SearchOption.TopDirectoryOnly)
            {
                NativeMethods.WIN32_FIND_DATA findData;
                Interop.NativeMethods.SafeFindHandle findHandle;

                findHandle = NativeMethods.FindFirstFile(LongPath.Combine(path.TrimEnd('\\'), searchPattern), out findData);
                int errorCode = Marshal.GetLastWin32Error();

                if (!findHandle.IsInvalid)
                {
                    if (findData.FileName != "."
                        && findData.FileName != "..")
                        yield return path + findData.FileName;

                    while (NativeMethods.FindNextFile(findHandle, out findData))
                    {
                        if (findData.FileName != "."
                            && findData.FileName != "..")
                        {
                            System.Console.WriteLine(path + findData.FileName);
                            yield return path + findData.FileName;
                        }
                    }

                    errorCode = Marshal.GetLastWin32Error();
                    if (findHandle != null)
                        findHandle.Dispose();

                    if ((0 != errorCode)
                        && (errorCode != NativeMethods.ERROR_NO_MORE_FILES)
                        && (errorCode != NativeMethods.ERROR_FILE_NOT_FOUND))
                    {
                        throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                }
                else
                {
                    if ((0 != errorCode)
                        && (183 != errorCode))
                    {
                        throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                }
            }
            else
            {
                throw new NotSupportedException(nameof(searchOption) + "is not supported.");
            }
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
            uint dwAttributes = NativeMethods.GetFileAttributes(path);
            int errorCode = Marshal.GetLastWin32Error();

            FileAttributes ret = new FileAttributes();
            if (dwAttributes > 0)
            {
                ret = (FileAttributes)dwAttributes;
                return ret;
            }

            if ((0 != errorCode))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            return ret;
#endif
        }
    }
}
