//------------------------------------------------------------------------------
// <copyright file="AzureFileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Transfer enumerator for Azure file storage.
    /// </summary>
    internal class AzureFileEnumerator : TransferEnumeratorBase, ITransferEnumerator
    {
        /// <summary>
        /// Configures how many entries to request in each ListFilesSegmented call from Azure Storage.
        /// Configuring a larger number will require fewer calls to Azure Storage, but each call will take longer to complete.
        /// Maximum supported by the Azure Storage API is 5000. Anything above this is rounded down to 5000.
        /// TODO: It's the same number used by blob, we need to find a proper number for azure file service.
        /// </summary>
        private const int ListFilesSegmentSize = 250;

        /// <summary>
        /// A file name (excluding container name) can be at most 255 character long based on Windows Azure documentation.
        /// </summary>
        private const int MaxDirectoryAndFileNameLength = 255;

        /// <summary>
        /// A cloud file path can be at most 1024 character long based on Windows Azure documentation.
        /// </summary>
        private const int MaxPathLength = 1024;

        /// <summary>
        /// Delimiter used in Uri.
        /// </summary>
        private const char UriDelimiter = '/';

        private AzureFileDirectoryLocation location;

        private AzureFileListContinuationToken listContinuationToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileLocation" /> class.
        /// </summary>
        /// <param name="location">Azure file directory location.</param>
        public AzureFileEnumerator(AzureFileDirectoryLocation location)
        {
            this.location = location;
        }

        /// <summary>
        /// Gets or sets the enumerate continulation token.
        /// </summary>
        public ListContinuationToken EnumerateContinuationToken
        {
            get
            {
                return this.listContinuationToken;
            }

            set
            {
                this.listContinuationToken = value as AzureFileListContinuationToken;
                Debug.Assert(null == value || null != this.listContinuationToken);
            }
        }

        /// <summary>
        /// Enumerates the cloud files in the Azure file location referenced by this object.
        /// </summary>
        /// <param name="cancellationToken">CancellationToken to cancel the method.</param>
        /// <returns>Enumerable list of TransferEntry objects found in the storage location referenced by this object.</returns>
        public IEnumerable<TransferEntry> EnumerateLocation(CancellationToken cancellationToken)
        {
            Utils.CheckCancellation(cancellationToken);

            if (this.Recursive)
            {
                // For recursive operation, file pattern is not supported.
                Debug.Assert(string.IsNullOrEmpty(this.SearchPattern), "filePattern");

                foreach (TransferEntry entry in this.EnumerateLocationRecursive(cancellationToken))
                {
                    yield return entry;
                }
            }
            else
            {
                // For non-recursive operation, file pattern is file name;
                foreach (TransferEntry entry in this.EnumerateLocationNonRecursive(this.SearchPattern, cancellationToken))
                {
                    yield return entry;
                }
            }
        }

        private IEnumerable<TransferEntry> EnumerateLocationNonRecursive(string fileName, CancellationToken cancellationToken)
        {
            Utils.CheckCancellation(cancellationToken);

            if (fileName == null || fileName.Length == 0 || fileName.Length > MaxDirectoryAndFileNameLength)
            {
                // Empty string or exceed-limit-length file name surely match no files.
                yield break;
            }

            if (this.listContinuationToken != null)
            {
                int compareResult = string.Compare(fileName, this.listContinuationToken.FilePath, StringComparison.Ordinal);
                if (compareResult <= 0)
                {
                    yield break;
                }
            }

            CloudFile cloudFile = this.location.FileDirectory.GetFileReference(fileName);
            FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
            ErrorEntry errorEntry = null;
            bool exist = false;

            try
            {
                exist = cloudFile.ExistsAsync(requestOptions, null, cancellationToken).Result;
            }
            catch (Exception ex)
            {
                string errorMessage = string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.FailedToEnumerateDirectory,
                    this.location.FileDirectory.SnapshotQualifiedUri.AbsoluteUri,
                    fileName);

                // Use TransferException to be more specific about the cloud file URI.
                TransferException exception =
                    new TransferException(TransferErrorCode.FailToEnumerateDirectory, errorMessage, ex);

                errorEntry = new ErrorEntry(exception);
            }

            if (null != errorEntry)
            {
                yield return errorEntry;
            }
            else if (exist)
            {
                yield return new AzureFileEntry(fileName, cloudFile, new AzureFileListContinuationToken(fileName));
            }
        }

        private IEnumerable<TransferEntry> EnumerateLocationRecursive(CancellationToken cancellationToken)
        {
            string fullPrefix = Uri.UnescapeDataString(this.location.FileDirectory.SnapshotQualifiedUri.AbsolutePath);

            // Normalize full prefix to end with slash.
            if (!string.IsNullOrEmpty(fullPrefix) && !fullPrefix.EndsWith("/", StringComparison.OrdinalIgnoreCase))
            {
                fullPrefix += '/';
            }

            Stack<CloudFileDirectory> directoriesToList = new Stack<CloudFileDirectory>();
            directoriesToList.Push(this.location.FileDirectory);

            string[] pathSegList = null;
            bool passedContinuationToken = false;
            int pathSegListIndex = 0;
            
            if (null != this.listContinuationToken)
            {
                pathSegList = this.listContinuationToken.FilePath.Split(new char[] { UriDelimiter });
            }
            else
            {
                passedContinuationToken = true;
            }

            while (0 != directoriesToList.Count)
            {
                CloudFileDirectory directory = directoriesToList.Pop();
                string dirAbsolutePath = Uri.UnescapeDataString(directory.SnapshotQualifiedUri.AbsolutePath);
                if (dirAbsolutePath[dirAbsolutePath.Length - 1] != UriDelimiter)
                {
                    dirAbsolutePath = dirAbsolutePath + UriDelimiter;
                }

                Stack<CloudFileDirectory> innerDirList = new Stack<CloudFileDirectory>();

                FileContinuationToken continuationToken = null;

                // To check whether reached continuation token by dir or file in this round.
                bool checkFile = false;
                bool passedSubFolder = false;
                string continuationTokenSeg = null;
                if (!passedContinuationToken)
                {
                    if (pathSegList.Length - 1 == pathSegListIndex)
                    {
                        checkFile = true;
                    }

                    continuationTokenSeg = pathSegList[pathSegListIndex];
                    pathSegListIndex++;
                }

                do
                {
                    FileResultSegment resultSegment = null;
                    Utils.CheckCancellation(cancellationToken);

                    ErrorEntry errorEntry = null;

                    try
                    {
                        FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                        resultSegment = directory.ListFilesAndDirectoriesSegmentedAsync(
                            ListFilesSegmentSize,
                            continuationToken,
                            requestOptions,
                            null,
                            cancellationToken).Result;
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.FailedToEnumerateDirectory,
                            directory.SnapshotQualifiedUri.AbsoluteUri,
                            string.Empty);

                        TransferException exception =
                            new TransferException(TransferErrorCode.FailToEnumerateDirectory, errorMessage, ex);
                        errorEntry = new ErrorEntry(exception);
                    }

                    if (null != errorEntry)
                    {
                        yield return errorEntry;
                        yield break;
                    }

                    continuationToken = resultSegment.ContinuationToken;

                    foreach (IListFileItem fileItem in resultSegment.Results)
                    {
                        Utils.CheckCancellation(cancellationToken);

                        if (fileItem is CloudFileDirectory)
                        {
                            if (checkFile || passedContinuationToken || passedSubFolder)
                            {
                                innerDirList.Push(fileItem as CloudFileDirectory);
                            }
                            else
                            {
                                CloudFileDirectory cloudDir = fileItem as CloudFileDirectory;
                                string fullPath = Uri.UnescapeDataString(cloudDir.SnapshotQualifiedUri.AbsolutePath);
                                string segName = fullPath.Remove(0, dirAbsolutePath.Length);

                                int compareResult = string.Compare(segName, continuationTokenSeg, StringComparison.OrdinalIgnoreCase);

                                if (compareResult >= 0)
                                {
                                    passedSubFolder = true;
                                    innerDirList.Push(cloudDir);

                                    if (compareResult > 0)
                                    {
                                        passedContinuationToken = true;
                                    }
                                }
                            }
                        }
                        else if (fileItem is CloudFile)
                        {
                            if (!checkFile && !passedContinuationToken)
                            {
                                continue;
                            }

                            CloudFile cloudFile = fileItem as CloudFile;

                            string fullPath = Uri.UnescapeDataString(cloudFile.SnapshotQualifiedUri.AbsolutePath);
                            string relativePath = fullPath.Remove(0, fullPrefix.Length);

                            if (passedContinuationToken)
                            {
                                yield return new AzureFileEntry(
                                    relativePath,
                                    cloudFile,
                                    new AzureFileListContinuationToken(relativePath));
                            }
                            else
                            {
                                string segName = fullPath.Remove(0, dirAbsolutePath.Length);
                                int compareResult = string.Compare(segName, continuationTokenSeg, StringComparison.OrdinalIgnoreCase);

                                if (compareResult < 0)
                                {
                                    continue;
                                }

                                passedContinuationToken = true;

                                if (compareResult > 0)
                                {
                                    yield return new AzureFileEntry(
                                        relativePath,
                                        cloudFile,
                                        new AzureFileListContinuationToken(relativePath));
                                }
                            }
                        }
                    }
                }
                while (continuationToken != null);

                if (checkFile)
                {
                    passedContinuationToken = true;
                }

                if (innerDirList.Count <= 0)
                {
                    if (!checkFile && !passedContinuationToken)
                    {
                        passedContinuationToken = true;
                    }
                }
                else
                {
                    while (innerDirList.Count > 0)
                    {
                        directoriesToList.Push(innerDirList.Pop());
                    }
                }
            }
        }
    }
}
