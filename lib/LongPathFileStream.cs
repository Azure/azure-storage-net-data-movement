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

#if !DOTNET5_4
    /// <summary>
    /// Internal calss to support long path files.
    /// </summary>
    internal class LongPathFileStream : Stream
    {
        private long position = 0;
        private string filePath = null;
        protected SafeFileHandle fileHandle = null;
        private FileAccess filePermission;

        public LongPathFileStream(string filePath, FileMode mode, FileAccess access, FileShare share)
        {
            this.filePath = LongPath.ToUncPath(filePath);
            this.filePermission = access;
            this.fileHandle = CreateFile(this.filePath, mode, access, share);
        }

        public override void Close()
        {
            if (this.fileHandle != null && !this.fileHandle.IsClosed)
            {
                this.fileHandle.Close();
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
            base.Dispose(true);
        }

        public override bool CanRead
        {
            get
            {
                if (this.fileHandle != null && !this.fileHandle.IsClosed
                    && (filePermission == FileAccess.Read || filePermission == FileAccess.ReadWrite))
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
                if (this.fileHandle != null && !this.fileHandle.IsClosed
                    && (filePermission == FileAccess.Write || filePermission == FileAccess.ReadWrite))
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
                    long size = 0;
                    if (!NativeMethods.GetFileSizeEx(this.fileHandle, out size))
                        NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
                    return size;
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
                if (!CanSeek)
                {
                    throw new NotSupportedException("Not able to seek");
                }
                if (this.position != value)
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
                throw new ArgumentOutOfRangeException(nameof(buffer), "ArgumentOutOfRange: offset + count out of buffer's range.");
            if (!CanRead)
                throw new NotSupportedException("NotSupported_UnableToReadTargetStream");

            uint read = 0;
            bool success = false;

            NativeOverlapped template = new NativeOverlapped();
            template.EventHandle = IntPtr.Zero;
            template.OffsetLow = (int)(Position & uint.MaxValue);
            template.OffsetHigh = (int)(Position >> 32);
            if (offset != 0)
            {
                var tempBuffer = new byte[count];
                success = NativeMethods.ReadFile(this.fileHandle, tempBuffer, (uint)(count), out read, ref template);
                tempBuffer.CopyTo(buffer, offset);
            }
            else
            {
                success = NativeMethods.ReadFile(this.fileHandle, buffer, (uint)(count), out read, ref template);
            }

            if (!success)
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(
                    new int[] {
                        NativeMethods.ERROR_SUCCESS,
                        NativeMethods.ERROR_HANDLE_EOF
                    });
            Position += read;
            return (int)(read);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (!CanSeek)
            {
                throw new InvalidOperationException("Current long path file stream is not able to seek.");
            }

            // Checked error message inside NativeMethods.Seek
            Position = NativeMethods.Seek(this.fileHandle, offset, origin);
            return Position;
        }

        public override void SetLength(long value)
        {
            Seek(value, SeekOrigin.Begin);
            if (!NativeMethods.SetEndOfFile(this.fileHandle))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
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
                throw new ArgumentOutOfRangeException(nameof(buffer), "ArgumentOutOfRange: offset + count out of buffer's range.");
            if (!CanWrite)
                throw new NotSupportedException("NotSupported_UnableToWriteTargetStream");

            uint written = 0;
            if (offset != 0)
            {
                buffer = buffer.Skip(offset).ToArray();
            }
            NativeOverlapped template = new NativeOverlapped();
            template.EventHandle = IntPtr.Zero;
            template.OffsetLow = (int)(Position & uint.MaxValue);
            template.OffsetHigh = (int)(Position >> 32);

            if (!NativeMethods.WriteFile(this.fileHandle, buffer, (uint)(count), out written, ref template))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
            Position += written;
        }

        private SafeFileHandle CreateFile(string path, FileMode mode, FileAccess access, FileShare share)
        {
            this.fileHandle = NativeMethods.CreateFileW(path,
                access,
                share,
                IntPtr.Zero,
                mode,
                FileAttributes.Normal,
                IntPtr.Zero);

            if (this.fileHandle.IsInvalid)
            {
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                    NativeMethods.ERROR_SUCCESS,
                    NativeMethods.ERROR_ALREADY_EXISTS
                });
            }
            return this.fileHandle;
        }
    }
#endif

    public static class LongPath
    {
        public static string ToUncPath(string localFilePath)
        {
            if (localFilePath == null)
                return null;

            string ret = localFilePath;
            if (!ret.StartsWith(@"\\", StringComparison.Ordinal))
            {
                ret = @"\\?\" + ret;
            }
            ret = LongPath.GetFullPath(localFilePath);
            if (!ret.StartsWith(@"\\", StringComparison.Ordinal))
            {
                return @"\\?\" + ret;
            }
            return ret;
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

        /// <summary>
        /// Returns the directory information for the specified path string.
        /// </summary>
        /// <param name="path">The path of a file or directory.</param>
        /// <returns></returns>
        public static string GetDirectoryName(string path)
        {
#if DOTNET5_4
            return Path.GetDirectoryName(path);
#else
            if (path != null)
            {
                int root = GetRootLength(path);
                int lastSeparator = path.Length;
                if (lastSeparator > root)
                {
                    while (lastSeparator > root && path[--lastSeparator] != Path.DirectorySeparatorChar && path[lastSeparator] != Path.AltDirectorySeparatorChar) ;
                    return path.Substring(0, lastSeparator);
                }
            }
            return null;
#endif
        }

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

            for (int i = path.Length - 1; i >= 0; --i)
            {
                char ch = path[i];
                if (ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar || ch == Path.VolumeSeparatorChar)
                    return i + 1;
            }

            return 0;
        }
    }

    public static class LongPathDirectory
    {
        public static bool Exists(string path)
        {
#if DOTNET5_4
            return Directory.Exists(path);
#else
            path = LongPath.ToUncPath(path);
            bool success = NativeMethods.PathFileExistsW(path);

            if (!success)
            {
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] { 0, NativeMethods.ERROR_DIRECTORY_NOT_FOUND, NativeMethods.ERROR_FILE_NOT_FOUND });
            }

            return success;
#endif
        }

#if !CODE_ACCESS_SECURITY
        public static string[] GetFiles(string path)
        {
#if DOTNET5_4
            return Directory.GetFiles(path);
#else
            return EnumerateFileSystemEntries(path, "*", SearchOption.TopDirectoryOnly, FilesOrDirectory.File).ToArray();
#endif
        }
#endif

        /// <summary>
        /// Creates all directories and subdirectories in the specified path unless they already exist.
        /// </summary>
        /// <param name="path">The directory to create.</param>
        public static void CreateDirectory(string path)
        {
#if DOTNET5_4
            Directory.CreateDirectory(path);
#else
            var dir = LongPath.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir) && !LongPathDirectory.Exists(dir))
            {
                LongPathDirectory.CreateDirectory(dir);
            }

            path = LongPath.ToUncPath(path);

            if (!NativeMethods.CreateDirectoryW(path, IntPtr.Zero))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                    NativeMethods.ERROR_SUCCESS,
                    NativeMethods.ERROR_ALREADY_EXISTS
                });
#endif
        }

        public enum FilesOrDirectory
        {
            None,
            File,
            Directory,
            All
        };
        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption)
        {
            return EnumerateFileSystemEntries(path, searchPattern, searchOption, FilesOrDirectory.All);
        }

        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption, FilesOrDirectory filter)
        {
#if DOTNET5_4
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
            NativeMethods.WIN32_FIND_DATA findData;
            Interop.NativeMethods.SafeFindHandle findHandle;
            string currentPath = null;

            Queue<string> folders = new Queue<string>();
            folders.Enqueue(path);
            while (folders.Count > 0)
            {
                currentPath = folders.Dequeue();
                findHandle = NativeMethods.FindFirstFileW(LongPath.Combine(LongPath.ToUncPath(currentPath), searchPattern), out findData);

                if (!findHandle.IsInvalid)
                {
                    if (findData.FileName != "."
                        && findData.FileName != "..")
                    {
                        if (searchOption == SearchOption.AllDirectories
                            && findData.FileAttributes == FileAttributes.Directory)
                        {
                            folders.Enqueue(LongPath.Combine(currentPath, findData.FileName));
                        }
                        if ((filter == FilesOrDirectory.All)
                            || (filter == FilesOrDirectory.Directory && findData.FileAttributes == FileAttributes.Directory)
                            || (filter == FilesOrDirectory.File && findData.FileAttributes != FileAttributes.Directory))
                            yield return LongPath.Combine(currentPath, findData.FileName);
                    }

                    while (NativeMethods.FindNextFileW(findHandle, out findData))
                    {
                        if (findData.FileName != "."
                            && findData.FileName != "..")
                        {
                            if (searchOption == SearchOption.AllDirectories
                                && findData.FileAttributes == FileAttributes.Directory)
                            {
                                folders.Enqueue(LongPath.Combine(currentPath, findData.FileName));
                            }
                            if ((filter == FilesOrDirectory.All)
                                || (filter == FilesOrDirectory.Directory && findData.FileAttributes == FileAttributes.Directory)
                                || (filter == FilesOrDirectory.File && findData.FileAttributes != FileAttributes.Directory))
                                yield return LongPath.Combine(currentPath, findData.FileName);
                        }
                    }

                    // Get last Win32 error right after native calls.
                    // Dispose SafeFindHandle will call native methods, it is possible to set last Win32 error.
                    var errorCode = Marshal.GetLastWin32Error();
                    if (findHandle != null)
                        findHandle.Dispose();

                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(errorCode,
                        new int[] {
                        NativeMethods.ERROR_SUCCESS,
                        NativeMethods.ERROR_NO_MORE_FILES,
                        NativeMethods.ERROR_FILE_NOT_FOUND
                    });
                }
                else
                {
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                        NativeMethods.ERROR_SUCCESS,
                        NativeMethods.ERROR_NO_MORE_FILES,
                        NativeMethods.ERROR_FILE_NOT_FOUND
                    });
                }
            }
#endif
        }
    }

    public static class LongPathFile
    {
        public static FileAttributes GetAttributes(string path)
        {
#if DOTNET5_4
            return File.GetAttributes(path);
#else
            uint dwAttributes = NativeMethods.GetFileAttributesW(path);

            FileAttributes ret = new FileAttributes();
            if (dwAttributes > 0)
            {
                ret = (FileAttributes)dwAttributes;
                return ret;
            }

            NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                NativeMethods.ERROR_SUCCESS
            });
            return ret;
#endif
        }
    }
}
