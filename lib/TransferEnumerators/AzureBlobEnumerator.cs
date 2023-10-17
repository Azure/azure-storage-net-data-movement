﻿//------------------------------------------------------------------------------
// <copyright file="AzureBlobEnumerator.cs" company="Microsoft">
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
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;

    /// <summary>
    /// Transfer enumerator for Azure blob storage.
    /// </summary>
    internal class AzureBlobEnumerator : TransferEnumeratorBase, ITransferEnumerator
    {
        /// <summary>
        /// A blob name (excluding container name) can be at most 1024 character long based on Windows Azure documentation.
        /// See <c>https://docs.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata</c> for details.
        /// </summary>
        private const int MaxBlobNameLength = 1024;

        /// <summary>
        /// Configures how many entries to request in each ListBlobsSegmented/ListFilesAndDirectoriesSegmented call from Azure Storage.
        /// Configuring a larger number will require fewer calls to Azure Storage, but each call will take longer to complete.
        /// Maximum supported by the Azure Storage API is 5000. Anything above this is rounded down to 5000.
        /// </summary>
        private const int DefaultListBlobsSegmentSize = 250;

        /// <summary>
        /// Initial attempt + all potential retries
        /// </summary>
        private const int DefaultMaxAttemptsPerSegment = 3;

        private static readonly TimeSpan DefaultRetryInterval = TimeSpan.FromSeconds(10);

        private readonly AzureBlobDirectoryLocation location;
        private readonly IDataMovementLogger logger;

        private AzureBlobListContinuationToken listContinuationToken;
        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobEnumerator" /> class.
        /// </summary>

        public AzureBlobEnumerator(AzureBlobDirectoryLocation location, IDataMovementLogger logger) 
        : this(location, logger, DefaultRetryInterval, DefaultMaxAttemptsPerSegment)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobEnumerator" /> class.
        /// </summary>
        public AzureBlobEnumerator(AzureBlobDirectoryLocation location, IDataMovementLogger logger, TimeSpan retryInterval, int maxRetryAttempts)
        {
            this.location = location;
            this.logger = logger;

            RetryInterval = retryInterval;
            MaxAttemptsPerPage = maxRetryAttempts;
        }

        /// <summary>
        /// Gets or sets the enumerate continuation token.
        /// </summary>
        public ListContinuationToken EnumerateContinuationToken
        {
            get
            {
                return this.listContinuationToken;
            }

            set
            {
                this.listContinuationToken = value as AzureBlobListContinuationToken;
                Debug.Assert(null == value || null != this.listContinuationToken);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether include blob snapshots when enumerating blobs.
        /// </summary>
        public bool IncludeSnapshots
        {
            get;
            set;
        }

        public TimeSpan RetryInterval
        {
            get;
        }

        public int MaxAttemptsPerPage
        {
            get;
        }

        public int? SegmentSize
        {
            set;
            get;
        }

        /// <summary>
        /// Enumerates the blobs present in the storage location referenced by this object.
        /// </summary>
        /// <param name="cancellationToken">CancellationToken to cancel the method.</param>
        /// <returns>Enumerable list of TransferEntry objects found in the storage location referenced by this object.</returns>
        public IEnumerable<TransferEntry> EnumerateLocation(CancellationToken cancellationToken)
        {
            Utils.CheckCancellation(cancellationToken);

            string filePattern = this.SearchPattern ?? string.Empty;

            // Exceed-limit-length patterns surely match no files.
            int maxFileNameLength = this.GetMaxFileNameLength();
            if (filePattern.Length > maxFileNameLength)
            {
                yield break;
            }

            CloudBlobContainer container = this.location.BlobDirectory.Container;
            BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
            BlobContinuationToken continuationToken = (this.listContinuationToken == null ? null : this.listContinuationToken.BlobContinuationToken);
            bool passedContinuationToken = (this.listContinuationToken == null);
            string dirPrefix = this.location.BlobDirectory.Prefix;
            string patternPrefix = dirPrefix + filePattern;

            do
            {
                BlobResultSegment resultSegment = null;

                ErrorEntry errorEntry = null;

                Utils.CheckCancellation(cancellationToken);
                try
                {
                    resultSegment = GetBlobsSegment(container, patternPrefix, continuationToken, requestOptions, cancellationToken);
                }
                catch (Exception ex)
                {
                    TransferException exception = new TransferException(
                        TransferErrorCode.FailToEnumerateDirectory, 
                        Resources.FailedToEnumerateDirectory, 
                        ex);

                    exception.Data.Add("directory", this.location.BlobDirectory.Uri.AbsoluteUri);
                    exception.Data.Add("pattern", filePattern);

                    errorEntry = new ErrorEntry(exception);
                }

                if (null != errorEntry)
                {
                    // Just return an error entry if we cannot access the container
                    yield return errorEntry;

                    // TODO: What should we do if some entries have been listed successfully?
                    yield break;
                }

                foreach (IListBlobItem blobItem in resultSegment.Results)
                {
                    Utils.CheckCancellation(cancellationToken);
                    CloudBlob blob = blobItem as CloudBlob;

                    if (null != blob)
                    {
                        if (!this.IncludeSnapshots && blob.SnapshotTime.HasValue)
                        {
                            continue;
                        }

                        if (!passedContinuationToken)
                        {
                            int compareResult = string.Compare(this.listContinuationToken.BlobName, blob.Name, StringComparison.Ordinal);
                            if (compareResult < 0)
                            {
                                passedContinuationToken = true;
                            }
                            else if (0 == compareResult)
                            {
                                if (IsSnapshotTimeEarlier(this.listContinuationToken.SnapshotTime, blob.SnapshotTime))
                                {
                                    passedContinuationToken = true;
                                }
                            }

                            if (!passedContinuationToken)
                            {
                                continue;
                            }
                        }

                        // TODO: currrently not support search for files with prefix specified without considering sub-directory.
                        bool returnItOrNot = this.Recursive ?
                            blob.Name.StartsWith(patternPrefix, StringComparison.Ordinal) :
                            blob.Name.Equals(patternPrefix, StringComparison.Ordinal);

                        if (returnItOrNot)
                        {
	                        if (IsDirectory(blob))
	                        {
		                        // just swallow the directory regardless is it empty or not - this part differs from original DML version
		                        continue;
	                        }

                            yield return new AzureBlobEntry(
                                blob.Name.Remove(0, dirPrefix.Length),
                                blob,
                                new AzureBlobListContinuationToken(continuationToken, blob.Name, blob.SnapshotTime));
                        }
                    }
                }

                continuationToken = resultSegment.ContinuationToken;
            }
            while (continuationToken != null);
        }

        private BlobResultSegment GetBlobsSegment(CloudBlobContainer container, string patternPrefix,
            BlobContinuationToken continuationToken, BlobRequestOptions requestOptions, CancellationToken cancellationToken)
        {
            var maxRetries = MaxAttemptsPerPage - 1;

            return RetryExecutor.ExecuteWithRetry(() =>
                    container.ListBlobsSegmentedAsync(
                    patternPrefix,
                    true,
                    BlobListingDetails.Snapshots | BlobListingDetails.Metadata,
                    SegmentSize ?? DefaultListBlobsSegmentSize,
                    continuationToken,
                    requestOptions,
                    null,
                    cancellationToken).Result,
                onRetry: (exceptionFromLastAttempt, retryCount) =>
                {
                    logger.Warning(exceptionFromLastAttempt, $"Remote enumeration failed. Retrying ({retryCount}/{maxRetries}).");
                },
            RetryInterval, MaxAttemptsPerPage);
        }

        /// <summary>
		/// Determines whether the specified blob is a directory based on its metadata.
		/// </summary>
		/// <returns>
		///   <c>true</c> if the specified blob is a directory; otherwise, <c>false</c>.
		/// </returns>
		private static bool IsDirectory(CloudBlob blob)
        {
	        return blob.Metadata.ContainsKey(Constants.DirectoryBlobMetadataKey);
        }

        /// <summary>
        /// Blob service returns snapshots from the ordest to the newest and the base blob is the last one.
        /// This method is to check whether the first one is returned before the second one.
        /// </summary>
        /// <param name="first">Snapshot time of the first blob.</param>
        /// <param name="second">Snapshot time of the second blob.</param>
        /// <returns>
        /// True: when the first one should be returned before the second one, 
        /// which means that the first one has a snapshot time and it's earlier that the second one's.
        /// Otherwise false.
        /// </returns>
        private static bool IsSnapshotTimeEarlier(DateTimeOffset? first, DateTimeOffset? second)
        {
            return !first.HasValue ? false : !second.HasValue ? true : DateTimeOffset.Compare(first.Value, second.Value) < 0;
        }

        /// <summary>
        /// Gets the maximum file name length of any blob relative to this objects storage location. 
        /// </summary>
        /// <returns>Maximum file name length in bytes.</returns>
        private int GetMaxFileNameLength()
        {
            return MaxBlobNameLength - this.location.BlobDirectory.Prefix.Length;
        }
    }
}
