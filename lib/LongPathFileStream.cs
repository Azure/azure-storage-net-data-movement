//------------------------------------------------------------------------------
// <copyright file="LongPathFileStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
#if !DOTNET5_4
    using Microsoft.Win32.SafeHandles;
#endif
    using System.Diagnostics;
    using Microsoft.Azure.Storage.DataMovement.Interop;

    internal static class LongPath
    {
        private const string ExtendedPathPrefix = @"\\?\";
        private const string UncPathPrefix = @"\\";
        private const string UncExtendedPrefixToInsert = @"?\UNC\";
        // \\?\, \\.\, \??\
        internal const int DevicePrefixLength = 4;

        /// <summary>
        /// Returns true if the path specified is relative to the current drive or working directory.
        /// Returns false if the path is fixed to a specific drive or UNC path.  This method does no
        /// validation of the path (URIs will be returned as relative as a result).
        /// </summary>
        internal static bool IsPartiallyQualified(string path)
        {
            if (path.Length < 2)
            {
                // It isn't fixed, it must be relative.  There is no way to specify a fixed
                // path with one character (or less).
                return true;
            }

            if (IsDirectorySeparator(path[0]))
            {
                // There is no valid way to specify a relative path with two initial slashes or
                // \? as ? isn't valid for drive relative paths and \??\ is equivalent to \\?\
                return !(path[1] == '?' || IsDirectorySeparator(path[1]));
            }

            // The only way to specify a fixed path that doesn't begin with two slashes
            // is the drive, colon, slash format- i.e. C:\
            return !((path.Length >= 3)
                && (path[1] == Path.VolumeSeparatorChar)
                && IsDirectorySeparator(path[2])
                // To match old behavior we'll check the drive character for validity as the path is technically
                // not qualified if you don't have a valid drive. "=:\" is the "=" file's default data stream.
                && IsValidDriveChar(path[0]));
        }

        /// <summary>
        /// Returns true if the given character is a valid drive letter
        /// </summary>
        internal static bool IsValidDriveChar(char value)
        {
            return ((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z'));
        }

        /// <summary>
        /// Returns true if the path uses any of the DOS device path syntaxes. ("\\.\", "\\?\", or "\??\")
        /// </summary>
        internal static bool IsDevice(string path)
        {
            // If the path begins with any two separators is will be recognized and normalized and prepped with
            // "\??\" for internal usage correctly. "\??\" is recognized and handled, "/??/" is not.
            return IsExtended(path)
                ||
                (
                    path.Length >= DevicePrefixLength
                    && IsDirectorySeparator(path[0])
                    && IsDirectorySeparator(path[1])
                    && (path[2] == '.' || path[2] == '?')
                    && IsDirectorySeparator(path[3])
                );
        }

        /// <summary>
        /// Returns true if the path uses the canonical form of extended syntax ("\\?\" or "\??\"). If the
        /// path matches exactly (cannot use alternate directory separators) Windows will skip normalization
        /// and path length checks.
        /// </summary>
        internal static bool IsExtended(string path)
        {
            // While paths like "//?/C:/" will work, they're treated the same as "\\.\" paths.
            // Skipping of normalization will *only* occur if back slashes ('\') are used.
            return path.Length >= DevicePrefixLength
                && path[0] == '\\'
                && (path[1] == '\\' || path[1] == '?')
                && path[2] == '?'
                && path[3] == '\\';
        }

        private static bool IsDirectorySeparator(char ch)
        {
            return ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar;
        }

        public static string ToUncPath(string path)
        {
            if (IsDevice(path))
            {
                return LongPath.GetFullPath(path);
            }

            if (IsPartiallyQualified(path))
            {
                path = LongPath.GetFullPath(path);
                if (IsDevice(path))
                    return path;
                else
                    return ExtendedPathPrefix + path;
            }

            //// Given \\server\share in longpath becomes \\?\UNC\server\share
            if (path.StartsWith(UncPathPrefix, StringComparison.OrdinalIgnoreCase))
                return LongPath.GetFullPath(path.Insert(2, UncExtendedPrefixToInsert));

            return LongPath.GetFullPath(ExtendedPathPrefix + path);
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
            if (actualSize == 0)
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
            if (actualSize > buffSize)
            {
                buffSize = (int)actualSize + 16;
                fullPath = new StringBuilder(buffSize);
                fileName = new StringBuilder(buffSize);
                actualSize = NativeMethods.GetFullPathNameW(path, (uint)buffSize, fullPath, fileName);
                if (actualSize == 0)
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
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

    internal static class LongPathDirectory
    {
        public static bool Exists(string path)
        {
#if DOTNET5_4
            return Directory.Exists(path);
#else
            try
            {

                if (String.IsNullOrEmpty(path))
                    return false;
                path = LongPath.ToUncPath(path);
                bool success = NativeMethods.PathFileExistsW(path);
                if (!success)
                {
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] { 0, NativeMethods.ERROR_DIRECTORY_NOT_FOUND, NativeMethods.ERROR_FILE_NOT_FOUND });
                }
                var fileAttributes = LongPathFile.GetAttributes(path);
                return success && (FileAttributes.Directory == (fileAttributes & FileAttributes.Directory));
            }
            catch (ArgumentException) { }
            catch (NotSupportedException) { }  // Security can throw this on ":"
            catch (System.Security.SecurityException) { }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }

            return false;
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
#if (DEBUG && TEST_HOOK)
            FaultInjectionPoint fip = new FaultInjectionPoint(FaultInjectionPoint.FIP_ThrowExceptionOnDirectory);
            string fiValue;

            if (fip.TryGetValue(out fiValue) && !String.IsNullOrEmpty(fiValue))
            {
                if (!fiValue.EndsWith(Path.DirectorySeparatorChar.ToString()))
                {
                    fiValue = fiValue + Path.DirectorySeparatorChar.ToString();
                }

                if (path.EndsWith(fiValue, StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception("test exception thrown because of FIP_ThrowExceptionOnDirectory is enabled");
                }
            }
#endif
#if DOTNET5_4
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
            return EnumerateFileSystemEntries(path, searchPattern, searchOption, FilesOrDirectory.All);
#endif
        }

        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption, FilesOrDirectory filter)
        {
#if DOTNET5_4
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
            NativeMethods.WIN32_FIND_DATA findData;
            NativeMethods.SafeFindHandle findHandle;
            string currentPath = null;
            int errorCode = 0;

            Queue<string> folders = new Queue<string>();
            String searchPath = LongPath.Combine(path, searchPattern);
            path = LongPath.GetDirectoryName(searchPath);
            searchPattern = LongPath.GetFileName(searchPath);
            folders.Enqueue(path);
            while (folders.Count > 0)
            {
                currentPath = folders.Dequeue();
                if (searchOption == SearchOption.AllDirectories)
                {
                    findHandle = NativeMethods.FindFirstFileW(LongPath.Combine(LongPath.ToUncPath(currentPath), "*"), out findData);
                    if (!findHandle.IsInvalid)
                    {
                        do
                        {
                            if (findData.FileName != "."
                                && findData.FileName != "..")
                            {
                                if (findData.FileAttributes == FileAttributes.Directory)
                                {
                                    folders.Enqueue(LongPath.Combine(currentPath, findData.FileName));
                                }
                            }
                        }
                        while (NativeMethods.FindNextFileW(findHandle, out findData));

                        // Get last Win32 error right after native calls.
                        // Dispose SafeFindHandle will call native methods, it is possible to set last Win32 error.
                        errorCode = Marshal.GetLastWin32Error();
                        if (findHandle != null
                            && !findHandle.IsInvalid)
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

                findHandle = NativeMethods.FindFirstFileW(LongPath.Combine(LongPath.ToUncPath(currentPath), searchPattern), out findData);
                if (!findHandle.IsInvalid)
                {
                    do
                    {
                        if (findData.FileName != "."
                            && findData.FileName != "..")
                        {
                            if ((filter == FilesOrDirectory.All)
                                || (filter == FilesOrDirectory.Directory && findData.FileAttributes == FileAttributes.Directory)
                                || (filter == FilesOrDirectory.File && findData.FileAttributes != FileAttributes.Directory))
                            {
                                yield return LongPath.Combine(currentPath, findData.FileName);
                            }
                        }
                    }
                    while (NativeMethods.FindNextFileW(findHandle, out findData));

                    // Get last Win32 error right after native calls.
                    // Dispose SafeFindHandle will call native methods, it is possible to set last Win32 error.
                    errorCode = Marshal.GetLastWin32Error();
                    if (findHandle != null
                        && !findHandle.IsInvalid)
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

    internal static partial class LongPathFile
    {
        public static FileStream Open(string filePath, FileMode mode, FileAccess access, FileShare share)
        {
#if DOTNET5_4
            return new FileStream(filePath, mode, access, share);
#else
            filePath = LongPath.ToUncPath(filePath);
            SafeFileHandle fileHandle = GetFileHandle(filePath, mode, access, share);
            return new FileStream(fileHandle, access);
#endif
        }

#if !DOTNET5_4
        private static SafeFileHandle GetFileHandle(string path, FileMode mode, FileAccess access, FileShare share, bool isDirectory =  false)
        {
            uint genericAccess = 0;
            switch (access)
            {
                case FileAccess.Read:
                    genericAccess = NativeMethods.GENERIC_READ;
                    break;
                case FileAccess.Write:
                    genericAccess = NativeMethods.GENERIC_WRITE;
                    break;
                case FileAccess.ReadWrite:
                    genericAccess = NativeMethods.GENERIC_READ_WRITE;
                    break;
            }

            uint attributesOrFlag = (uint)(FileAttributes.Normal);

            if (isDirectory)
            {
                attributesOrFlag |= NativeMethods.FILE_FLAG_BACKUP_SEMANTICS;
            }

            SafeFileHandle fileHandle = NativeMethods.GetFileHandleW(path,
                genericAccess,
                share,
                IntPtr.Zero,
                mode,
                attributesOrFlag,
                IntPtr.Zero);

            if (fileHandle.IsInvalid)
            {
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                    NativeMethods.ERROR_SUCCESS,
                    NativeMethods.ERROR_ALREADY_EXISTS
                });
            }
            return fileHandle;
        }
#endif
        public static void SetAttributes(string path, FileAttributes fileAttributes)
        {
#if DEBUG
            if (null != TestHookCallbacks.SetFileAttributesCallback)
            {
                TestHookCallbacks.SetFileAttributesCallback(path, fileAttributes);
                return;
            }
#endif

#if DOTNET5_4
            File.SetAttributes(path, fileAttributes);
#else
            path = LongPath.ToUncPath(path);
            if (!NativeMethods.SetFileAttributesW(path, (uint)(fileAttributes)))
            {
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
            }
#endif
        }

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

        public static void GetFileProperties(string path, out DateTimeOffset? creationTime, out DateTimeOffset? lastWriteTime, out FileAttributes? fileAttributes
#if DOTNET5_4
            , bool isDirectory = false
#endif
            )
        {
#if !DOTNET5_4
            path = LongPath.ToUncPath(path);

            if (path.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal))
            {
                path = path.Substring(0, path.Length - 1);
            }

            NativeMethods.WIN32_FIND_DATA findData;
            NativeMethods.SafeFindHandle findHandle;

            findHandle = NativeMethods.FindFirstFileW(path, out findData);

            if (findHandle.IsInvalid)
            {
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
            }
            else
            {
                findHandle.Dispose();
                findHandle = null;
            }

            long dt = (findData.LastWriteTime.dwLowDateTime & 0xFFFFFFFF);
            dt |= ((long)findData.LastWriteTime.dwHighDateTime) << 32;
            lastWriteTime = DateTimeOffset.FromFileTime(dt).UtcDateTime;

            dt = (findData.CreationTime.dwLowDateTime & 0xFFFFFFFF);
            dt |= ((long)findData.CreationTime.dwHighDateTime) << 32;
            creationTime = DateTimeOffset.FromFileTime(dt).UtcDateTime;

            fileAttributes = (FileAttributes)findData.FileAttributes;
#else
            fileAttributes = File.GetAttributes(path);

            if (isDirectory)
            {
                creationTime = Directory.GetCreationTimeUtc(path);
                lastWriteTime = Directory.GetLastWriteTimeUtc(path);
            }
            else
            {
                creationTime = File.GetCreationTimeUtc(path);
                lastWriteTime = File.GetLastWriteTimeUtc(path);
            }
#endif

#if DEBUG
            if (null != TestHookCallbacks.GetFileAttributesCallback)
            {
                fileAttributes = TestHookCallbacks.GetFileAttributesCallback(path);
            }
#endif
        }

        public static void SetFileTime(string path, DateTimeOffset creationTimeUtc, DateTimeOffset lastWriteTimeUtc, bool isDirectory = false)
        {
#if !DOTNET5_4
            path = LongPath.ToUncPath(path);
            SafeFileHandle fileHandle = GetFileHandle(path, FileMode.Open, FileAccess.Write, FileShare.None, isDirectory);

            try
            {
                NativeMethods.FILETIME ftCreationTime = new NativeMethods.FILETIME();
                NativeMethods.FILETIME ftLastAccessTime = new NativeMethods.FILETIME();
                NativeMethods.FILETIME ftLastWriteTime = new NativeMethods.FILETIME();
                long dt = lastWriteTimeUtc.UtcDateTime.ToFileTimeUtc();
                ftLastWriteTime.dwLowDateTime = (uint)dt & 0xFFFFFFFF;
                ftLastWriteTime.dwHighDateTime = (uint)(dt >> 32) & 0xFFFFFFFF;

                dt = creationTimeUtc.UtcDateTime.ToFileTimeUtc();
                ftCreationTime.dwLowDateTime = (uint)dt & 0xFFFFFFFF;
                ftCreationTime.dwHighDateTime = (uint)(dt >> 32) & 0xFFFFFFFF;

                if (true != NativeMethods.SetFileTime(fileHandle, ref ftCreationTime, ref ftLastAccessTime, ref ftLastWriteTime))
                {
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
                }
            }
            finally
            {
                fileHandle.Close();
                fileHandle = null;
            }
#else
            if (isDirectory)
            {
                Directory.SetCreationTimeUtc(path, creationTimeUtc.UtcDateTime);
                Directory.SetLastWriteTimeUtc(path, lastWriteTimeUtc.UtcDateTime);
            }
            else
            {
                File.SetCreationTimeUtc(path, creationTimeUtc.UtcDateTime);
                File.SetLastWriteTimeUtc(path, lastWriteTimeUtc.UtcDateTime);
            }
#endif
        }
    }
}
