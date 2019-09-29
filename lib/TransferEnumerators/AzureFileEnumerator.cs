//------------------------------------------------------------------------------
// <copyright file="AzureFileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Transfer enumerator for Azure file storage.
    /// A hierarchy enumerator will return files and directories which are directly under current directory.
    /// </summary>
    internal class AzureFileHierarchyEnumerator : TransferEnumeratorBase, ITransferEnumerator
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

        private CloudFileDirectory baseDirectory;

        private AzureFileListContinuationToken listContinuationToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileLocation" /> class.
        /// </summary>
        /// <param name="location">Azure file directory location.</param>
        /// <param name="baseDirectory"></param>
        public AzureFileHierarchyEnumerator(AzureFileDirectoryLocation location, CloudFileDirectory baseDirectory)
        {
            this.location = location;
            this.baseDirectory = baseDirectory;
        }

        /// <summary>
        /// Gets or sets the enumerate continulation token.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
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
        /// Enumerates the files in the transfer location referenced by this object.
        /// </summary>
        /// <param name="cancellationToken">CancellationToken to notify the method cancellation.</param>
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
            string fullPrefix = null;
            if (null != this.baseDirectory)
            {
                fullPrefix = Uri.UnescapeDataString(this.baseDirectory.SnapshotQualifiedUri.AbsolutePath);
            }
            else
            {
                fullPrefix = Uri.UnescapeDataString(this.location.FileDirectory.SnapshotQualifiedUri.AbsolutePath);
            }

            // Normalize full prefix to end with slash.
            if (!string.IsNullOrEmpty(fullPrefix) && !fullPrefix.EndsWith("/", StringComparison.OrdinalIgnoreCase))
            {
                fullPrefix += '/';
            }

            CloudFileDirectory directory = this.location.FileDirectory;

            Stack<CloudFileDirectory> innerDirList = new Stack<CloudFileDirectory>();

            FileContinuationToken continuationToken = null;
            bool passedContinuationToken = false;
            if (null == this.listContinuationToken)
            {
                passedContinuationToken = true;
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
                        cancellationToken).GetAwaiter().GetResult();
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
                        CloudFileDirectory cloudDir = fileItem as CloudFileDirectory;

                        if (!passedContinuationToken)
                        {
                            if (string.Equals(cloudDir.Name, this.listContinuationToken.FilePath, StringComparison.Ordinal))
                            {
                                passedContinuationToken = true;
                                continue;
                            }
                            else
                            {
                                continue;
                            }
                        }
                        string fullPath = Uri.UnescapeDataString(cloudDir.SnapshotQualifiedUri.AbsolutePath);
                        string relativePath = fullPath.Remove(0, fullPrefix.Length);

                        yield return new AzureFileDirectoryEntry(
                            relativePath,
                            cloudDir,
                            new AzureFileListContinuationToken(cloudDir.Name));
                    }
                    else if (fileItem is CloudFile)
                    {
                        CloudFile cloudFile = fileItem as CloudFile;

                        if (!passedContinuationToken)
                        {
                            if (string.Equals(cloudFile.Name, this.listContinuationToken.FilePath, StringComparison.Ordinal))
                            {
                                passedContinuationToken = true;
                                continue;
                            }
                            else
                            {
                                continue;
                            }
                        }

                        string fullPath = Uri.UnescapeDataString(cloudFile.SnapshotQualifiedUri.AbsolutePath);
                        string relativePath = fullPath.Remove(0, fullPrefix.Length);

                        yield return new AzureFileEntry(
                            relativePath,
                            cloudFile,
                            new AzureFileListContinuationToken(cloudFile.Name));
                    }
                }
            }
            while (continuationToken != null);
        }
    }
}
