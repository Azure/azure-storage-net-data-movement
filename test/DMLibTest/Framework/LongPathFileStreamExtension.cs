//------------------------------------------------------------------------------
// <copyright file="LongPathFileStreamExtentionExtention.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Framework
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
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.DataMovement.Interop;

    public static class LongPathExtension
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
                return LongPathExtension.GetFullPath(path);
            }

            if (IsPartiallyQualified(path))
            {
                path = LongPathExtension.GetFullPath(path);
                if (IsDevice(path))
                    return path;
                else
                    return ExtendedPathPrefix + path;
            }

            //// Given \\server\share in longpath becomes \\?\UNC\server\share
            if (path.StartsWith(UncPathPrefix, StringComparison.OrdinalIgnoreCase))
                return LongPathExtension.GetFullPath(path.Insert(2, UncExtendedPrefixToInsert));

            return LongPathExtension.GetFullPath(ExtendedPathPrefix + path);
        }

        public static string GetFullPath(string path)
        {
#if DOTNET5_4
            return Path.GetFullPath(path);
#else
            return LongPath.GetFullPath(path);
#endif
        }

        public static string Combine(string path1, string path2)
        {
#if DOTNET5_4
            return Path.Combine(path1, path2);
#else
            return LongPath.Combine(path1, path2);
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
            return LongPath.GetDirectoryName(path);
#endif
        }

        public static string GetFileNameWithoutExtension(string path)
        {
#if DOTNET5_4
            return Path.GetFileNameWithoutExtension(path);
#else
            return LongPath.GetFileNameWithoutExtension(path);
#endif
        }

        public static string GetFileName(string path)
        {
#if DOTNET5_4
            return Path.GetFileName(path);
#else
            return LongPath.GetFileName(path);
#endif
        }
    }

    internal class LongPathDirectoryExtension
    {
        private LongPathDirectoryExtension() { }

        public static bool Exists(string path)
        {
#if DOTNET5_4
            return Directory.Exists(path);
#else
            return LongPathDirectory.Exists(path);
#endif
        }

        public static string[] GetFiles(string path, SearchOption searchOption = SearchOption.TopDirectoryOnly)
        {
#if DOTNET5_4
            return Directory.GetFiles(path);
#else
            return EnumerateFileSystemEntries(path, "*", searchOption, LongPathDirectory.FilesOrDirectory.File).ToArray();
#endif
        }

        public static string[] GetDirectories(string path, SearchOption searchOption = SearchOption.TopDirectoryOnly)
        {
#if DOTNET5_4
            return Directory.GetDirectories(path);
#else
            return EnumerateFileSystemEntries(path, "*", searchOption, LongPathDirectory.FilesOrDirectory.Directory).ToArray();
#endif
        }

        /// <summary>
        /// Creates all directories and subdirectories in the specified path unless they already exist.
        /// </summary>
        /// <param name="path">The directory to create.</param>
        public static void CreateDirectory(string path)
        {
#if DOTNET5_4
            Directory.CreateDirectory(path);
#else
            LongPathDirectory.CreateDirectory(path);
#endif
        }

        public static void Delete(string path)
        {
#if DOTNET5_4
            Directory.Delete(path);
#else
            path = LongPathExtension.ToUncPath(path);
            if (!NativeMethods.RemoveDirectoryW(path))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
#endif
        }

        public static void Delete(string path, bool recursive = false)
        {
#if DOTNET5_4
            Directory.Delete(path, recursive);
#else
            path = LongPathExtension.ToUncPath(path);
            if (recursive == true)
            {
                string[] dirs = GetDirectories(path, SearchOption.AllDirectories);
                string[] files = GetFiles(path, SearchOption.AllDirectories);

                Array.Sort(dirs, (string strA, string strB) => {
                    if (strA.Length > strB.Length)
                        return -1;
                    if (strA.Length < strB.Length)
                        return 1;
                    return string.Compare(strB, strA);
                });

                Array.Sort(files, (string strA, string strB) => {
                    if (strA.Length > strB.Length)
                        return -1;
                    if (strA.Length < strB.Length)
                        return 1;
                    return string.Compare(strB, strA);
                });

                foreach (var subPath in files)
                {
                    LongPathFileExtension.Delete(subPath);
                }

                foreach (var subDir in dirs)
                {
                    LongPathDirectoryExtension.Delete(subDir);
                }
            }
            LongPathDirectoryExtension.Delete(path);
#endif
        }

#if DOTNET5_4
        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption)
        {
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
#else
        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption, LongPathDirectory.FilesOrDirectory filter = LongPathDirectory.FilesOrDirectory.All)
        {
            return LongPathDirectory.EnumerateFileSystemEntries(path, searchPattern, searchOption, filter);
#endif
        }
    }

    internal class LongPathFileExtension
    {
        private LongPathFileExtension() { }

        public static FileAttributes GetAttributes(string path)
        {
#if DOTNET5_4
            return File.GetAttributes(path);
#else
            return Microsoft.Azure.Storage.DataMovement.LongPathFile.GetAttributes(path);
#endif
        }

        public static bool Exists(string path)
        {
#if DOTNET5_4
            return File.Exists(path);
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
                var fileAttributes = Microsoft.Azure.Storage.DataMovement.LongPathFile.GetAttributes(path);
                return success && (FileAttributes.Directory != (fileAttributes & FileAttributes.Directory));
            }
            catch (ArgumentException) { }
            catch (NotSupportedException) { }  // Security can throw this on ":"
            catch (System.Security.SecurityException) { }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }

            return false;
#endif
        }

        public static void Delete(string path)
        {
#if DOTNET5_4
            File.Delete(path);
#else
            path = LongPathExtension.ToUncPath(path);
            if (!NativeMethods.DeleteFileW(path))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
#endif
        }

        public static FileStream Create(string path)
        {
            return Open(path, FileMode.Create);
        }

        public static FileStream Open(string path, FileMode mode, FileAccess access, FileShare share)
        {
            return LongPathFile.Open(path, mode, access, share);
        }

        public static FileStream Open(string path, FileMode mode)
        {
            return LongPathFile.Open(path, mode, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        public static void SetAttributes(string path, FileAttributes fileAttributes)
        {
            path = LongPath.ToUncPath(path);
            LongPathFile.SetAttributes(path, fileAttributes);
        }

        public static void GetFileProperties(string path, out DateTimeOffset? creationTime, out DateTimeOffset? lastWriteTime, out FileAttributes? fileAttributes
#if DOTNET5_4
            , bool isDirectory = false
#endif
            )
        {
            path = LongPath.ToUncPath(path);
#if DOTNET5_4
            LongPathFile.GetFileProperties(path, out creationTime, out lastWriteTime, out fileAttributes, isDirectory);
#else
            LongPathFile.GetFileProperties(path, out creationTime, out lastWriteTime, out fileAttributes);
#endif
        }
    }
}
