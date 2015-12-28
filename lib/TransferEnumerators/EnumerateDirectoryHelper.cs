//------------------------------------------------------------------------------
// <copyright file="EnumerateDirectoryHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Security.Permissions;
    using System.Threading;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Inter-op methods for enumerating files and directory.
    /// </summary>
    internal static class EnumerateDirectoryHelper
    {
        /// <summary>
        /// Returns the names of files (including their paths) in the specified directory that match the specified 
        /// search pattern, using a value to determine whether to search subdirectories.
        /// Folder permission will be checked for those folders containing found files.
        /// Difference with Directory.GetFiles/EnumerateFiles: Junctions and folders not accessible will be ignored.
        /// </summary>
        /// <param name="path">The directory to search. </param>
        /// <param name="searchPattern">The search string to match against the names of files in path. The parameter 
        /// cannot end in two periods ("..") or contain two periods ("..") followed by DirectorySeparatorChar or 
        /// AltDirectorySeparatorChar, nor can it contain any of the characters in InvalidPathChars. </param>
        /// <param name="fromFilePath">Enumerate from this file, file(s) before this and this file won't be 
        /// returned.</param>
        /// <param name="searchOption">One of the values of the SearchOption enumeration that specifies whether 
        /// the search operation should include only the current directory or should include all subdirectories.
        /// The default value is TopDirectoryOnly.</param>
        /// <param name="cancellationToken">CancellationToken to cancel the method.</param>
        /// <returns>An enumerable collection of file names in the directory specified by path and that match 
        /// searchPattern and searchOption.</returns>
        public static IEnumerable<string> EnumerateFiles(
            string path,
            string searchPattern,
            string fromFilePath,
            SearchOption searchOption,
            CancellationToken cancellationToken)
        {
            Utils.CheckCancellation(cancellationToken);

            if ((searchOption != SearchOption.TopDirectoryOnly) && (searchOption != SearchOption.AllDirectories))
            {
                throw new ArgumentOutOfRangeException("searchOption");
            }

            // Remove whitespaces in the end.
            searchPattern = searchPattern.TrimEnd();
            if (searchPattern.Length == 0)
            {
                // Returns an empty string collection.
                return new List<string>();
            }

            // To support patterns like "folderA\" aiming at listing files under some folder.
            if ("." == searchPattern)
            {
                searchPattern = "*";
            }

            Utils.CheckCancellation(cancellationToken);

            CheckSearchPattern(searchPattern);

            Utils.CheckCancellation(cancellationToken);

            // Check path permissions.
            string fullPath = Path.GetFullPath(path);
            CheckPathDiscoveryPermission(fullPath);

            string patternDirectory = Path.GetDirectoryName(searchPattern);
            if (!string.IsNullOrEmpty(patternDirectory))
            {
                CheckPathDiscoveryPermission(Path.Combine(fullPath, patternDirectory));
            }

            if (!string.IsNullOrEmpty(fromFilePath)
                && !string.IsNullOrEmpty(patternDirectory))
            {
                // if file pattern is like folder\fileName*, we'll list location\folder with pattern fileName*
                // but the listted relative path will still be like folder\fileName1, and the continuation token will look the same.
                // Then here we need to make continuation token to be path relative to location\folder.
                string tmpPatternDir = AppendDirectorySeparator(patternDirectory);
                fromFilePath = fromFilePath.Substring(tmpPatternDir.Length);
            }

            string fullPathWithPattern = Path.Combine(fullPath, searchPattern);

            // To support patterns like "folderA\" aiming at listing files under some folder.
            char lastC = fullPathWithPattern[fullPathWithPattern.Length - 1];
            if (Path.DirectorySeparatorChar == lastC ||
                Path.AltDirectorySeparatorChar == lastC ||
                Path.VolumeSeparatorChar == lastC)
            {
                fullPathWithPattern = fullPathWithPattern + '*';
            }

            string directoryName = AppendDirectorySeparator(Path.GetDirectoryName(fullPathWithPattern));
            string filePattern = fullPathWithPattern.Substring(directoryName.Length);

            Utils.CheckCancellation(cancellationToken);
            return InternalEnumerateFiles(directoryName, filePattern, fromFilePath, searchOption, cancellationToken);
        }

        private static IEnumerable<string> InternalEnumerateFiles(
            string directoryName,
            string filePattern,
            string fromFilePath,
            SearchOption searchOption,
            CancellationToken cancellationToken)
        {
            Stack<string> folders = new Stack<string>();
            Stack<string> currentFolderSubFolders = new Stack<string>();
            folders.Push(directoryName);

            string[] pathSegList = null;
            bool passedContinuationToken = false;
            int pathSegListIndex = 0;

            if (null != fromFilePath)
            {
                pathSegList = fromFilePath.Split(new char[] { Path.DirectorySeparatorChar });
            }
            else
            {
                passedContinuationToken = true;
            }

            while (folders.Count > 0)
            {
                string folder = AppendDirectorySeparator(folders.Pop());

                Utils.CheckCancellation(cancellationToken);

                try
                {
                    CheckPathDiscoveryPermission(folder);
                }
                catch (SecurityException)
                {
                    // Ignore this folder if we have no right to discovery it.
                    continue;
                }

                NativeMethods.WIN32_FIND_DATA findFileData;

                if (passedContinuationToken
                    || (pathSegList.Length - 1 == pathSegListIndex))
                {
                    string continuationTokenFile = null;

                    if (!passedContinuationToken)
                    {
                        continuationTokenFile = pathSegList[pathSegListIndex];
                    }

                    // Load files directly under this folder.
                    using (var findHandle = NativeMethods.FindFirstFile(folder + filePattern, out findFileData))
                    {
                        if (!findHandle.IsInvalid)
                        {
                            do
                            {
                                Utils.CheckCancellation(cancellationToken);

                                if (FileAttributes.Directory != (findFileData.FileAttributes & FileAttributes.Directory))
                                {
                                    if (passedContinuationToken)
                                    {
                                        yield return Path.Combine(folder, findFileData.FileName);
                                    }
                                    else
                                    {
                                        int compareResult = string.Compare(findFileData.FileName, continuationTokenFile, StringComparison.OrdinalIgnoreCase);
                                        if (compareResult < 0)
                                        {
                                            continue;
                                        }

                                        passedContinuationToken = true;

                                        if (compareResult > 0)
                                        {
                                            yield return Path.Combine(folder, findFileData.FileName);
                                        }
                                    }
                                }
                            }
                            while (NativeMethods.FindNextFile(findHandle, out findFileData));
                        }
                    }

                    // Passed folder which continuation token file is under,
                    // set passedContinuationToken to true.
                    passedContinuationToken = true;
                }

                if (SearchOption.AllDirectories == searchOption)
                {
                    string fromSubfolder = null;
                    bool passedSubfoler = passedContinuationToken;
                    if (!passedContinuationToken)
                    {
                        fromSubfolder = pathSegList[pathSegListIndex];
                        pathSegListIndex++;
                    }

                    // Add sub-folders.
                    using (var findHandle = NativeMethods.FindFirstFile(folder + '*', out findFileData))
                    {
                        if (!findHandle.IsInvalid)
                        {
                            do
                            {
                                Utils.CheckCancellation(cancellationToken);

                                if (FileAttributes.Directory == (findFileData.FileAttributes & FileAttributes.Directory) &&
                                    !findFileData.FileName.Equals(@".") &&
                                    !findFileData.FileName.Equals(@".."))
                                {
                                    // TODO: Ignore junction point or not. Make it configurable.
                                    if (FileAttributes.ReparsePoint != (findFileData.FileAttributes & FileAttributes.ReparsePoint))
                                    {
                                        if (passedSubfoler)
                                        {
                                            currentFolderSubFolders.Push(Path.Combine(folder, findFileData.FileName));
                                        }
                                        else
                                        {
                                            int compareResult = string.Compare(findFileData.FileName, fromSubfolder, StringComparison.OrdinalIgnoreCase);

                                            if (compareResult >= 0)
                                            {
                                                passedSubfoler = true;
                                                currentFolderSubFolders.Push(Path.Combine(folder, findFileData.FileName));

                                                if (compareResult > 0)
                                                {
                                                    passedContinuationToken = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            while (NativeMethods.FindNextFile(findHandle, out findFileData));
                        }
                    }

                    if (currentFolderSubFolders.Count <= 0)
                    {
                        passedContinuationToken = true;
                    }
                    else
                    {
                        while (currentFolderSubFolders.Count > 0)
                        {
                            folders.Push(currentFolderSubFolders.Pop());
                        }
                    }
                }
            }
        }

        private static string AppendDirectorySeparator(string dir)
        {
            char lastC = dir[dir.Length - 1];
            if (Path.DirectorySeparatorChar != lastC && Path.AltDirectorySeparatorChar != lastC)
            {
                dir = dir + Path.DirectorySeparatorChar;
            }

            return dir;
        }

        private static void CheckSearchPattern(string searchPattern)
        {
            while (true)
            {
                int index = searchPattern.IndexOf("..", StringComparison.Ordinal);

                if (-1 == index)
                {
                    return;
                }

                index += 2;

                if (searchPattern.Length == index ||
                    searchPattern[index] == Path.DirectorySeparatorChar ||
                    searchPattern[index] == Path.AltDirectorySeparatorChar)
                {
                    throw new ArgumentException(
                        "Search pattern cannot contain \"..\" to move up directories" +
                        "and can be contained only internally in file/directory names, " +
                        "as in \"a..b\"");
                }

                searchPattern = searchPattern.Substring(index);
            }
        }

        private static void CheckPathDiscoveryPermission(string dir)
        {
            string checkDir = AppendDirectorySeparator(dir) + '.';

            new FileIOPermission(FileIOPermissionAccess.PathDiscovery, checkDir).Demand();
        }

        /// <summary>
        /// Defines all native methods.
        /// </summary>
        internal static class NativeMethods
        {
            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
            public static extern SafeFindHandle FindFirstFile(string fileName, out WIN32_FIND_DATA findFileData);

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool FindNextFile(SafeHandle findFileHandle, out WIN32_FIND_DATA findFileData);

            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool FindClose(SafeHandle findFileHandle);

            [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto), BestFitMapping(false)]
            public struct WIN32_FIND_DATA
            {
                public FileAttributes FileAttributes;
                public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
                public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
                public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
                public uint FileSizeHigh;
                public uint FileSizeLow;
                public int Reserved0;
                public int Reserved1;
                [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
                public string FileName;
                [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 14)]
                public string AlternateFileName;
            }

            public sealed class SafeFindHandle : SafeHandleZeroOrMinusOneIsInvalid
            {
                [SecurityCritical]
                internal SafeFindHandle()
                    : base(true)
                {
                }

                protected override bool ReleaseHandle()
                {
                    if (!(this.IsInvalid || this.IsClosed))
                    {
                        return NativeMethods.FindClose(this);
                    }

                    return this.IsInvalid || this.IsClosed;
                }

                protected override void Dispose(bool disposing)
                {
                    if (!(this.IsInvalid || this.IsClosed))
                    {
                        NativeMethods.FindClose(this);
                    }

                    base.Dispose(disposing);
                }
            }
        }
    }
}
