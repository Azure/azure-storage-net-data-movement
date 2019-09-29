//------------------------------------------------------------------------------
// <copyright file="EnumerateDirectoryHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Security;
    using System.Threading;
    using Microsoft.Azure.Storage.DataMovement.Interop;
#if DOTNET5_4
    using Mono.Unix;
#else
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
        internal class EnumerateFileEntryInfo
        {
            public string FileName { get; set; }
            public FileAttributes FileAttributes { get; set; }
            public string SymlinkTarget { get; set; }
        };

        internal class LocalEnumerateItem
        {
            public string Path { get; set; }
            public bool IsDirectory { get; set; }
        }

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
        /// <param name="followsymlink">Indicating whether to enumerate symlinked subdirectories.</param>
        /// <param name="isBaseDirectory"></param>
        /// <param name="cancellationToken">CancellationToken to cancel the method.</param>
        /// <returns>An enumerable collection of file names in the directory specified by path and that match 
        /// searchPattern and searchOption.</returns>
#if TRANSPARENCY_V2
        // CAS versions of the library demand path discovery permission to EnumerateFiles, so when using
        // transparency (instead of CAS) make sure that partially trusted code cannot call this method.
        [SecurityCritical]
#endif // TRANSPARENCY_V2
        public static IEnumerable<LocalEnumerateItem> EnumerateAllEntriesInDirectory(
            string path,
            string searchPattern,
            string fromFilePath,
            SearchOption searchOption,
            bool followsymlink,
            bool isBaseDirectory,
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
                return new List<LocalEnumerateItem>();
            }

            // To support patterns like "folderA\" aiming at listing files under some folder.
            if ("." == searchPattern)
            {
                searchPattern = "*";
            }

            // Check path permissions.
            string fullPath = null;
            if (Interop.CrossPlatformHelpers.IsWindows)
            {
                fullPath = LongPath.ToUncPath(path);
            }
            else
            {
                fullPath = Path.GetFullPath(path);
            }

            string filePattern = null;
            string directoryName = AppendDirectorySeparator(fullPath);

            if (isBaseDirectory)
            {
                CheckSearchPattern(searchPattern);

                string fullPathWithPattern = LongPath.Combine(fullPath, searchPattern);

                // To support patterns like "folderA\" aiming at listing files under some folder.
                char lastC = fullPathWithPattern[fullPathWithPattern.Length - 1];
                if (Path.DirectorySeparatorChar == lastC ||
                    Path.AltDirectorySeparatorChar == lastC ||
                    Path.VolumeSeparatorChar == lastC)
                {
                    fullPathWithPattern = fullPathWithPattern + '*';
                }

                directoryName = AppendDirectorySeparator(LongPath.GetDirectoryName(fullPathWithPattern));
                filePattern = fullPathWithPattern.Substring(directoryName.Length);

                if (!LongPathDirectory.Exists(directoryName))
                {
                    throw new DirectoryNotFoundException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.PathNotFound,
                            directoryName));
                }

#if CODE_ACCESS_SECURITY
                CheckPathDiscoveryPermission(directoryName);
#endif // CODE_ACCESS_SECURITY

                string patternDirectory = LongPath.GetDirectoryName(searchPattern);
                if (!string.IsNullOrEmpty(fromFilePath)
                    && !string.IsNullOrEmpty(patternDirectory))
                {
                    // if file pattern is like folder\fileName*, we'll list location\folder with pattern fileName*
                    // but the listted relative path will still be like folder\fileName1, and the continuation token will look the same.
                    // Then here we need to make continuation token to be path relative to location\folder.
                    string tmpPatternDir = AppendDirectorySeparator(patternDirectory);
                    fromFilePath = fromFilePath.Substring(tmpPatternDir.Length);
                }
            }
            else
            {
                filePattern = searchPattern;
                string dirName = LongPath.GetDirectoryName(searchPattern);

                if (!string.IsNullOrEmpty(dirName))
                {
                    string patternDirName = AppendDirectorySeparator(dirName);
                    filePattern = searchPattern.Substring(patternDirName.Length);
                }

                if (filePattern.Length == 0)
                {
                    filePattern = "*";
                }
            }

            Utils.CheckCancellation(cancellationToken);
            return InternalEnumerateInDirectory(directoryName, filePattern, fromFilePath, searchOption, followsymlink, true, cancellationToken);
        }

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
        /// <param name="followsymlink">Indicating whether to enumerate symlinked subdirectories.</param>
        /// <param name="cancellationToken">CancellationToken to cancel the method.</param>
        /// <returns>An enumerable collection of file names in the directory specified by path and that match 
        /// searchPattern and searchOption.</returns>
#if TRANSPARENCY_V2
        // CAS versions of the library demand path discovery permission to EnumerateFiles, so when using
        // transparency (instead of CAS) make sure that partially trusted code cannot call this method.
        [SecurityCritical]
#endif // TRANSPARENCY_V2
        public static IEnumerable<LocalEnumerateItem> EnumerateInDirectory(
            string path,
            string searchPattern,
            string fromFilePath,
            SearchOption searchOption,
            bool followsymlink,
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
                return new List<LocalEnumerateItem>();
            }

            // To support patterns like "folderA\" aiming at listing files under some folder.
            if ("." == searchPattern)
            {
                searchPattern = "*";
            }

            Utils.CheckCancellation(cancellationToken);

            CheckSearchPattern(searchPattern);

            // Check path permissions.
            string fullPath = null;
            if(Interop.CrossPlatformHelpers.IsWindows)
            {
                fullPath = LongPath.ToUncPath(path);
            }
            else
            {
                fullPath = Path.GetFullPath(path);
            }
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
            return InternalEnumerateInDirectory(directoryName, filePattern, fromFilePath, searchOption, followsymlink, false, cancellationToken);
        }

#if TRANSPARENCY_V2
        [SecurityCritical]
#endif // TRANSPARENCY_V2
        private static IEnumerable<LocalEnumerateItem> InternalEnumerateInDirectory(
            string directoryName,
            string filePattern,
            string fromFilePath,
            SearchOption searchOption,
            bool followsymlink,
            bool returnDirectories,
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
#if DOTNET5_4
                    if (CrossPlatformHelpers.IsLinux)
                    {
                        if (!SymlinkedDirExists(folder))
                        {
                            continue;
                        }
                    }
                    else
                    {
                        continue;
                    }
#else
                    continue;
#endif
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
                catch (UnauthorizedAccessException)
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
                    foreach (var fileItem in LongPathDirectory.EnumerateFileSystemEntries(folder, "*", SearchOption.TopDirectoryOnly))
                    {
                        // Just try to get the first item in directly to check whether has permission to access the directory.
                        break;
                    }
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
                catch (Exception ex)
                {
                    throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.EnumerateDirectoryException, folder), ex);
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
                    foreach (var filePath in Utils.CatchException(() =>
                        {
                            return LongPathDirectory.EnumerateFileSystemEntries(folder, filePattern, SearchOption.TopDirectoryOnly);
                        },
                        (ex) =>
                        {
                            throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.EnumerateDirectoryException, folder), ex);
                        }))
                    {
                        Utils.CheckCancellation(cancellationToken);

                        EnumerateFileEntryInfo fileEntryInfo = null;

                        try
                        {
#if DOTNET5_4
                            if (CrossPlatformHelpers.IsLinux)
                            {
                                fileEntryInfo = GetFileEntryInfo(filePath);
                            }
                            else
                            {
                                fileEntryInfo = new EnumerateFileEntryInfo()
                                {
                                    FileName = LongPath.GetFileName(filePath),
                                    FileAttributes = LongPathFile.GetAttributes(filePath)
                                };
                            }
#else
                            fileEntryInfo = new EnumerateFileEntryInfo()
                            {
                                FileName = LongPath.GetFileName(filePath),
                                FileAttributes = LongPathFile.GetAttributes(filePath)
                            };
#endif
                        }
                        // Cross-plat file system accessibility settings may cause exceptions while
                        // retrieving attributes from inaccessible paths. These paths shold be skipped.
                        catch (FileNotFoundException) { }
                        catch (IOException) { }
                        catch (UnauthorizedAccessException) { }
                        catch (Exception ex)
                        {
                            throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.FailedToGetFileInfoException, filePath), ex);
                        }

                        if (null == fileEntryInfo)
                        {
                            continue;
                        }

                        if (FileAttributes.Directory != (fileEntryInfo.FileAttributes & FileAttributes.Directory))
                        {
                            if (passedContinuationToken)
                            {
                                yield return new LocalEnumerateItem()
                                {
                                    Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                    IsDirectory = false
                                };

                            }
                            else
                            {
                                if (CrossPlatformHelpers.IsLinux)
                                {
                                    if (!passedContinuationToken)
                                    {
                                        if (string.Equals(fileEntryInfo.FileName, continuationTokenFile, StringComparison.Ordinal))
                                        {
                                            passedContinuationToken = true;
                                        }
                                    }
                                    else
                                    {
                                        yield return new LocalEnumerateItem()
                                        {
                                            Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                            IsDirectory = false
                                        };
                                    }
                                }
                                else
                                {
                                    // Windows file system is case-insensitive; OSX and Linux are case-sensitive
                                    var comparison = CrossPlatformHelpers.IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
                                    int compareResult = string.Compare(fileEntryInfo.FileName, continuationTokenFile, comparison);
                                    if (compareResult < 0)
                                    {
                                        // Skip files prior to the continuation token file
                                        continue;
                                    }

                                    passedContinuationToken = true;

                                    if (compareResult > 0)
                                    {
                                        yield return new LocalEnumerateItem()
                                        {
                                            Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                            IsDirectory = false
                                        };
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
                    foreach (var filePath in Utils.CatchException(() =>
                        {
                            return LongPathDirectory.EnumerateFileSystemEntries(folder, "*", SearchOption.TopDirectoryOnly);
                        },
                        (ex) =>
                        {
                            throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.EnumerateDirectoryException, folder), ex);
                        }))
                    {
                        Utils.CheckCancellation(cancellationToken);

                        EnumerateFileEntryInfo fileEntryInfo = null;

                        try
                        {
                            if (CrossPlatformHelpers.IsLinux)
                            {
                                fileEntryInfo = GetFileEntryInfo(filePath);
                            }
                            else
                            {
                                fileEntryInfo = new EnumerateFileEntryInfo()
                                {
                                    FileName = LongPath.GetFileName(filePath),
                                    FileAttributes = LongPathFile.GetAttributes(filePath)
                                };
                            }
                        }
                        // Cross-plat file system accessibility settings may cause exceptions while
                        // retrieving attributes from inaccessible paths. These paths shold be skipped.
                        catch (FileNotFoundException) { }
                        catch (IOException) { }
                        catch (UnauthorizedAccessException) { }
                        catch (Exception ex)
                        {
                            throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.FailedToGetFileInfoException, filePath), ex);
                        }

                        if (null == fileEntryInfo)
                        {
                            continue;
                        }

                        if (FileAttributes.Directory == (fileEntryInfo.FileAttributes & FileAttributes.Directory) &&
                            !fileEntryInfo.FileName.Equals(@".") &&
                            !fileEntryInfo.FileName.Equals(@".."))
                        {
                            bool toBeEnumerated = false;
                            if (CrossPlatformHelpers.IsLinux)
                            {
                                toBeEnumerated = ToEnumerateTheSubDir(LongPath.Combine(folder, fileEntryInfo.FileName), fileEntryInfo, followsymlink);
                            }
                            // TODO: Ignore junction point or not. Make it configurable.
                            else if (FileAttributes.ReparsePoint != (fileEntryInfo.FileAttributes & FileAttributes.ReparsePoint))
                            {
                                toBeEnumerated = true;
                            }

                            if (toBeEnumerated)
                            {
                                if (passedSubfoler)
                                {
                                    if (returnDirectories)
                                    {
                                        yield return new LocalEnumerateItem()
                                        {
                                            Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                            IsDirectory = true
                                        };
                                    }
                                    else
                                    {
                                        currentFolderSubFolders.Push(LongPath.Combine(folder, fileEntryInfo.FileName));
                                    }
                                }
                                else
                                {
                                    if (CrossPlatformHelpers.IsLinux)
                                    {
                                        if (string.Equals(fileEntryInfo.FileName, fromSubfolder, StringComparison.Ordinal))
                                        {
                                            passedSubfoler = true;

                                            if (returnDirectories)
                                            {
                                                yield return new LocalEnumerateItem()
                                                {
                                                    Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                                    IsDirectory = true
                                                };
                                            }
                                            else
                                            {
                                                currentFolderSubFolders.Push(LongPath.Combine(folder, fileEntryInfo.FileName));
                                            }
                                        }
                                    }
                                    else
                                    {
                                        // Windows file system is case-insensitive; OSX and Linux are case-sensitive
                                        var comparison = CrossPlatformHelpers.IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
                                        int compareResult = string.Compare(fileEntryInfo.FileName, fromSubfolder, comparison);

                                        if (compareResult >= 0)
                                        {
                                            passedSubfoler = true;

                                            if (returnDirectories)
                                            {
                                                yield return new LocalEnumerateItem()
                                                {
                                                    Path = LongPath.Combine(folder, fileEntryInfo.FileName),
                                                    IsDirectory = true
                                                };
                                            }
                                            else
                                            {
                                                currentFolderSubFolders.Push(LongPath.Combine(folder, fileEntryInfo.FileName));
                                            }

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

        private static EnumerateFileEntryInfo GetFileEntryInfo(string filePath)
        {
            EnumerateFileEntryInfo fileEntryInfo = new EnumerateFileEntryInfo()
            {
                FileName = LongPath.GetFileName(filePath),
                FileAttributes = FileAttributes.Normal,
                SymlinkTarget = null
            };

#if DOTNET5_4
            try
            {
                UnixFileSystemInfo fileSystemInfo = UnixFileSystemInfo.GetFileSystemEntry(filePath);
                if (fileSystemInfo.IsSymbolicLink)
                {
                    fileEntryInfo.FileAttributes |= FileAttributes.ReparsePoint;
                    fileEntryInfo.SymlinkTarget = Path.GetFullPath(Path.Combine(GetParentPath(filePath), (fileSystemInfo as UnixSymbolicLinkInfo).ContentsPath));

                    UnixSymbolicLinkInfo symlinkInfo = fileSystemInfo as UnixSymbolicLinkInfo;

                    try
                    {
                        if (symlinkInfo.HasContents && symlinkInfo.GetContents().IsDirectory)
                        {
                            fileEntryInfo.FileAttributes |= FileAttributes.Directory;
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        // Just ignore exception thrown here. 
                        // later there will be "FileNotFoundException" thrown out when trying to open the file before transferring.
                    }
                }

                if (fileSystemInfo.IsDirectory)
                {
                    fileEntryInfo.FileAttributes |= FileAttributes.Directory;
                }
            }
            catch (DllNotFoundException ex)
            {
                throw new TransferException(TransferErrorCode.FailToEnumerateDirectory,
                    Resources.UnableToLoadDLL,
                    ex);
            }
#endif
            return fileEntryInfo;
        }

        private static bool ToEnumerateTheSubDir(string dirPath, EnumerateFileEntryInfo fileEntryInfo, bool followSymlink)
        {
            if (FileAttributes.ReparsePoint != (fileEntryInfo.FileAttributes & FileAttributes.ReparsePoint))
            {
                return true;
            }
            else if (followSymlink)
            {
                string fullPath = Path.GetFullPath(dirPath);

                if (fullPath.StartsWith(AppendDirectorySeparator(fileEntryInfo.SymlinkTarget), StringComparison.Ordinal))
                {
                    throw new TransferException(string.Format(CultureInfo.CurrentCulture, Resources.DeadLoop, fullPath, fileEntryInfo.SymlinkTarget));
                }

                return true;
            }

            return false;
        }

#if DOTNET5_4
        private static bool SymlinkedDirExists(string dirPath)
        {
            dirPath = dirPath.TrimEnd(Path.DirectorySeparatorChar);
            try
            {
                UnixFileSystemInfo fileSystemInfo = UnixFileSystemInfo.GetFileSystemEntry(dirPath);
                if (!fileSystemInfo.IsSymbolicLink)
                {
                    return false;
                }

                UnixSymbolicLinkInfo symlinkInfo = fileSystemInfo as UnixSymbolicLinkInfo;
                if (symlinkInfo.HasContents && symlinkInfo.GetContents().IsDirectory)
                {
                    return true;
                }
            }
            catch (DllNotFoundException ex)
            {
                throw new TransferException(TransferErrorCode.FailToEnumerateDirectory,
                    Resources.UnableToLoadDLL,
                    ex);
            }

            return false;
        }
        
        private static string GetParentPath(string filePath)
        {
            if (filePath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal))
            {
                filePath = filePath.Substring(0, filePath.Length - 1);
            }
            return Path.GetDirectoryName(filePath);
        }
#endif

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
            // Use the search pattern '*' instead.
            string checkDir = AppendDirectorySeparator(dir) + '*';
            Interop.NativeMethods.SafeFindHandle findHandle = null;
            try
            {
                Interop.NativeMethods.WIN32_FIND_DATA findData;

                findHandle = Interop.NativeMethods.FindFirstFileW(checkDir, out findData);
                int errorCode = Marshal.GetLastWin32Error();
                if (findHandle.IsInvalid)
                {
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(errorCode,
                        new int[] {
                        NativeMethods.ERROR_SUCCESS,
                        NativeMethods.ERROR_NO_MORE_FILES,
                        NativeMethods.ERROR_FILE_NOT_FOUND
                    });
                    throw new SecurityException("Request for the permission to list files.");
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
