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

    internal class LongPathFileStreamExtention : FileStream
    {
        public LongPathFileStreamExtention(string filePath, FileMode mode, FileAccess access, FileShare share) : base(filePath, mode, access, share)
        {
        }

        public LongPathFileStreamExtention(string filePath, FileMode mode) : base(filePath, mode, FileAccess.ReadWrite, FileShare.ReadWrite)
        {
        }
    }

#else
    /// <summary>
    /// Internal calss to support long path files.
    /// </summary
    internal class LongPathFileStreamExtention : LongPathFileStream
    {

        public LongPathFileStreamExtention(string filePath, FileMode mode, FileAccess access, FileShare share):base(filePath, mode, access, share)
        {
        }

        public LongPathFileStreamExtention(string filePath, FileMode mode):base(filePath, mode, FileAccess.ReadWrite, FileShare.ReadWrite)
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

    public static class LongPathExtention
    {
        public static string ToUncPath(string localFilePath)
        {
            return LongPath.ToUncPath(localFilePath);
        }

        public static string GetFullPath(string path)
        {
            return LongPath.GetFullPath(path);
        }

        public static string Combine(string path1, string path2)
        {
            return LongPath.Combine(path1, path2);
        }

        /// <summary>
        /// Returns the directory information for the specified path string.
        /// </summary>
        /// <param name="path">The path of a file or directory.</param>
        /// <returns></returns>
        public static string GetDirectoryName(string path)
        {
            return LongPath.GetDirectoryName(path);
        }

        public static string GetFileNameWithoutExtension(string path)
        {
            return LongPath.GetFileNameWithoutExtension(path);
        }

        public static string GetFileName(string path)
        {
            return LongPath.GetFileName(path);
        }
    }

    internal class LongPathDirectoryExtention
    {
        private LongPathDirectoryExtention() { }

        public static bool Exists(string path)
        {
            return LongPathDirectory.Exists(path);
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
            return Directory.GetFiles(path);
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
            LongPathDirectory.CreateDirectory(path);
        }

        public static void Delete(string path)
        {
#if DOTNET5_4
            Directory.Delete(path);
#else
            path = LongPathExtention.ToUncPath(path);
            if (!NativeMethods.RemoveDirectoryW(path))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
#endif
        }

        public static void Delete(string path, bool recursive = false)
        {
#if DOTNET5_4
            Directory.Delete(path, false);
#else
            path = LongPathExtention.ToUncPath(path);
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
                    LongPathFileExtention.Delete(subPath);
                }

                foreach (var subDir in dirs)
                {
                    LongPathDirectoryExtention.Delete(subDir);
                }
            }
            LongPathDirectoryExtention.Delete(path);
#endif
        }

        // Only SearchOption.TopDirectoryOnly is supported.
        // SearchOption.AllDirectories is not supported.
        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption, LongPathDirectory.FilesOrDirectory filter = LongPathDirectory.FilesOrDirectory.All)
        {
            return LongPathDirectory.EnumerateFileSystemEntries(path, searchPattern, searchOption, filter);
        }
    }

    internal class LongPathFileExtention
    {
        private LongPathFileExtention() { }

        public static FileAttributes GetAttributes(string path)
        {
            return LongPathFile.GetAttributes(path);
        }

        public static bool Exists(string path)
        {
#if DOTNET5_4
            return File.Exists(path);
#else
            return LongPathDirectoryExtention.Exists(path);
#endif
        }

        public static void Delete(string path)
        {
#if DOTNET5_4
            File.Delete(path);
#else
            path = LongPathExtention.ToUncPath(path);
            if (!NativeMethods.DeleteFileW(path))
                NativeMethods.ThrowExceptionForLastWin32ErrorIfExists();
#endif
        }

        public static LongPathFileStreamExtention Create(string path)
        {
            return new LongPathFileStreamExtention(path, FileMode.Create);
        }

        public static LongPathFileStreamExtention Open(string path, FileMode mode)
        {
            return new LongPathFileStreamExtention(path, mode);
        }

        public static LongPathFileStreamExtention Open(string path, FileMode mode, FileAccess access, FileShare share)
        {
            return new LongPathFileStreamExtention(path, mode, access, share);
        }
    }
}
