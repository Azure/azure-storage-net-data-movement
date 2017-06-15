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
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;

#if DNXCORE50

    internal class LongPathFileStreamExtension : FileStream
    {
        public LongPathFileStreamExtension(string filePath, FileMode mode, FileAccess access, FileShare share) : base(filePath, mode, access, share)
        {
        }

        public LongPathFileStreamExtension(string filePath, FileMode mode) : base(filePath, mode, FileAccess.ReadWrite, FileShare.ReadWrite)
        {
        }
    }

#else
    /// <summary>
    /// Internal calss to support long path files.
    /// </summary
    internal class LongPathFileStreamExtension : LongPathFileStream
    {

        public LongPathFileStreamExtension(string filePath, FileMode mode, FileAccess access, FileShare share):base(filePath, mode, access, share)
        {
        }

        public LongPathFileStreamExtension(string filePath, FileMode mode):base(filePath, mode, FileAccess.ReadWrite, FileShare.ReadWrite)
        {
        }

        public SafeHandle Handle
        {
            get
            {
                return base.fileHandle;
            }
        }

        public new void Dispose()
        {
            this.Close();
            base.Close();
        }
    }
#endif

    public static class LongPathExtension
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
            ret = LongPathExtension.GetFullPath(localFilePath);
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
            return LongPathFile.GetAttributes(path);
#endif
        }

        public static bool Exists(string path)
        {
#if DOTNET5_4
            return File.Exists(path);
#else
            return LongPathDirectoryExtension.Exists(path);
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

        public static LongPathFileStreamExtension Create(string path)
        {
            return new LongPathFileStreamExtension(path, FileMode.Create);
        }

        public static LongPathFileStreamExtension Open(string path, FileMode mode)
        {
            return new LongPathFileStreamExtension(path, mode);
        }

        public static LongPathFileStreamExtension Open(string path, FileMode mode, FileAccess access, FileShare share)
        {
            return new LongPathFileStreamExtension(path, mode, access, share);
        }
    }
}
