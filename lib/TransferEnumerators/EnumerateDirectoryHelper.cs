//------------------------------------------------------------------------------
// <copyright file="EnumerateDirectoryHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Security;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;

#if !DOTNET5_4
    using System.Runtime.InteropServices;
#endif

#if CODE_ACCESS_SECURITY
    using System.Security.Permissions;
#endif // CODE_ACCESS_SECURITY

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
#if TRANSPARENCY_V2
        // CAS versions of the library demand path discovery permission to EnumerateFiles, so when using
        // transparency (instead of CAS) make sure that partially trusted code cannot call this method.
        [SecurityCritical]
#endif // TRANSPARENCY_V2
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
            string fullPath =  LongPathFileStream.ToUncPath(path);
#if CODE_ACCESS_SECURITY
            CheckPathDiscoveryPermission(fullPath);
#endif // CODE_ACCESS_SECURITY

            string patternDirectory = LongPath.GetDirectoryName(searchPattern);
#if CODE_ACCESS_SECURITY
            if (!string.IsNullOrEmpty(patternDirectory))
            {
                CheckPathDiscoveryPermission(LongPath.Combine(fullPath, patternDirectory));
            }
#endif // CODE_ACCESS_SECURITY

            if (!string.IsNullOrEmpty(fromFilePath)
                && !string.IsNullOrEmpty(patternDirectory))
            {
                // if file pattern is like folder\fileName*, we'll list location\folder with pattern fileName*
                // but the listted relative path will still be like folder\fileName1, and the continuation token will look the same.
                // Then here we need to make continuation token to be path relative to location\folder.
                string tmpPatternDir = AppendDirectorySeparator(patternDirectory);
                fromFilePath = fromFilePath.Substring(tmpPatternDir.Length);
            }

            // string fullPathWithPattern = Path.Combine(fullPath, searchPattern);
            string fullPathWithPattern = LongPath.Combine(fullPath, searchPattern);

            // To support patterns like "folderA\" aiming at listing files under some folder.
            char lastC = fullPathWithPattern[fullPathWithPattern.Length - 1];
            if (Path.DirectorySeparatorChar == lastC ||
                Path.AltDirectorySeparatorChar == lastC ||
                Path.VolumeSeparatorChar == lastC)
            {
                fullPathWithPattern = fullPathWithPattern + '*';
            }

            string directoryName = AppendDirectorySeparator(LongPath.GetDirectoryName(fullPathWithPattern));
            string filePattern = fullPathWithPattern.Substring(directoryName.Length);

            if (!LongPathDirectory.Exists(directoryName))
            {
                throw new DirectoryNotFoundException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.PathNotFound,
                        directoryName));
            }

            Utils.CheckCancellation(cancellationToken);
            return InternalEnumerateFiles(directoryName, filePattern, fromFilePath, searchOption, cancellationToken);
        }

#if TRANSPARENCY_V2
        [SecurityCritical]
#endif // TRANSPARENCY_V2
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

                // Skip non-existent folders
                if (!LongPathDirectory.Exists(folder))
                {
                    continue;
                }

#if CODE_ACCESS_SECURITY // Only accessible to fully trusted code in non-CAS models
                try
                {
                    CheckPathDiscoveryPermission(folder);
                }
                catch (SecurityException)
                {
                    // Ignore this folder if we have no right to discovery it.
                    continue;
                }
#else // CODE_ACCESS_SECURITY
                try
                {
                    // In non-CAS scenarios, it's still important to check for folder accessibility
                    // since the OS can block access to some paths. Getting files from a location
                    // will force path discovery checks which will indicate whether or not the user
                    // is authorized to access the directory.
                    LongPathDirectory.GetFiles(folder);
                }
                catch (SecurityException)
                {
                    // Ignore this folder if we have no right to discovery it.
                    continue;
                }
                catch (UnauthorizedAccessException)
                {
                    // Ignore this folder if we have no right to discovery it.
                    continue;
                }
#endif // CODE_ACCESS_SECURITY

                // Return all files contained directly in this folder (which occur after the continuationTokenFile)
                // Only consider the folder if the continuation token is already past or the continuation token may be passed by
                // a file within this directory (based on pathSegListIndex and the patSegList.Length)
                if (passedContinuationToken
                    || (pathSegList.Length - 1 == pathSegListIndex))
                {
                    string continuationTokenFile = null;

                    if (!passedContinuationToken)
                    {
                        continuationTokenFile = pathSegList[pathSegListIndex];
                    }

                    // Load files directly under this folder.
                    foreach (var filePath in LongPathDirectory.EnumerateFileSystemEntries(folder, filePattern, SearchOption.TopDirectoryOnly))
                    {
                        Utils.CheckCancellation(cancellationToken);

                        FileAttributes fileAttributes = FileAttributes.Normal;
                        string fileName = null;

                        try
                        {
                            fileName = LongPath.GetFileName(filePath);
                            fileAttributes = LongPathFile.GetAttributes(filePath);
                        }
                        // Cross-plat file system accessibility settings may cause exceptions while
                        // retrieving attributes from inaccessible paths. These paths shold be skipped.
                        catch (FileNotFoundException) { }
                        catch (IOException) { }
                        catch (UnauthorizedAccessException) { }

                        if (fileName == null)
                        {
                            continue;
                        }

                        if (FileAttributes.Directory != (fileAttributes & FileAttributes.Directory))
                        {
                            if (passedContinuationToken)
                            {
                                yield return LongPath.Combine(folder, fileName);
                            }
                            else
                            {
                                if (CrossPlatformHelpers.IsLinux)
                                {
                                    if (!passedContinuationToken)
                                    {
                                        if (string.Equals(fileName, continuationTokenFile, StringComparison.Ordinal))
                                        {
                                            passedContinuationToken = true;
                                        }
                                    }
                                    else
                                    {
                                        yield return LongPath.Combine(folder, fileName);
                                    }
                                }
                                else
                                {
                                    // Windows file system is case-insensitive; OSX and Linux are case-sensitive
                                    var comparison = CrossPlatformHelpers.IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
                                    int compareResult = string.Compare(fileName, continuationTokenFile, comparison);
                                    if (compareResult < 0)
                                    {
                                        // Skip files prior to the continuation token file
                                        continue;
                                    }

                                    passedContinuationToken = true;

                                    if (compareResult > 0)
                                    {
                                        yield return LongPath.Combine(folder, fileName);
                                    }
                                }
                            }
                        }
                    }

                    // Passed folder which continuation token file is under,
                    // set passedContinuationToken to true.
                    passedContinuationToken = true;
                }

                // Next add sub-folders for processing
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
                    foreach (var filePath in LongPathDirectory.EnumerateFileSystemEntries(folder, "*", SearchOption.TopDirectoryOnly))
                    {
                        Utils.CheckCancellation(cancellationToken);

                        FileAttributes fileAttributes = FileAttributes.Normal;
                        string fileName = null;

                        try
                        {
                            fileName = LongPath.GetFileName(filePath);
                            fileAttributes = LongPathFile.GetAttributes(filePath);
                        }
                        // Cross-plat file system accessibility settings may cause exceptions while
                        // retrieving attributes from inaccessible paths. These paths shold be skipped.
                        catch (FileNotFoundException) { }
                        catch (IOException) { }
                        catch (UnauthorizedAccessException) { }

                        if (fileName == null)
                        {
                            continue;
                        }

                        if (FileAttributes.Directory == (fileAttributes & FileAttributes.Directory) &&
                            !fileName.Equals(@".") &&
                            !fileName.Equals(@".."))
                        {
                            // TODO: Ignore junction point or not. Make it configurable.
                            if (FileAttributes.ReparsePoint != (fileAttributes & FileAttributes.ReparsePoint))
                            {
                                if (passedSubfoler)
                                {
                                    currentFolderSubFolders.Push(LongPath.Combine(folder, fileName));
                                }
                                else
                                {
                                    if (CrossPlatformHelpers.IsLinux)
                                    {
                                        if (string.Equals(fileName, fromSubfolder, StringComparison.Ordinal))
                                        {
                                            passedSubfoler = true;
                                            currentFolderSubFolders.Push(LongPath.Combine(folder, fileName));
                                        }
                                    }
                                    else
                                    {
                                        // Windows file system is case-insensitive; OSX and Linux are case-sensitive
                                        var comparison = CrossPlatformHelpers.IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
                                        int compareResult = string.Compare(fileName, fromSubfolder, comparison);

                                        if (compareResult >= 0)
                                        {
                                            passedSubfoler = true;
                                            currentFolderSubFolders.Push(LongPath.Combine(folder, fileName));

                                            if (compareResult > 0)
                                            {
                                                passedContinuationToken = true;
                                            }
                                        }
                                    }
                                }
                            }
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

#if CODE_ACCESS_SECURITY
        private static void CheckPathDiscoveryPermission(string dir)
        {

#if DOTNET5_4
            string checkDir = AppendDirectorySeparator(dir) + '.';

            new FileIOPermission(FileIOPermissionAccess.PathDiscovery, checkDir).Demand();
#else
            // Prepending the string "\\?\" does not allow access to the root directory.
            string checkDir = AppendDirectorySeparator(dir) + '*';
            Interop.NativeMethods.SafeFindHandle findHandle = null;
            try
            {
                Interop.NativeMethods.WIN32_FIND_DATA findData;

                findHandle = Interop.NativeMethods.FindFirstFile(checkDir, out findData);
                int errorCode = Marshal.GetLastWin32Error();
                if (findHandle.IsInvalid)
                {
                    if (0 != errorCode
                        && 18 != errorCode)
                    {
                        throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                    throw new SecurityException();
                }

            }
            finally
            {
                if (findHandle != null
                    && !findHandle.IsInvalid)
                    findHandle.Dispose();
            }
#endif
        }
#endif // CODE_ACCESS_SECURITY
    }
}
