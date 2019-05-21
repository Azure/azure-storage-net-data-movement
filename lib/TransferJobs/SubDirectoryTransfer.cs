//------------------------------------------------------------------------------
// <copyright file="SubDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.Azure.Storage.File;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    class SubDirectoryTransfer : JournalItem
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string SubDirListContinuationTokenName = "SubDirListContinuationToken";
        private const string SubDirRelativePathName = "SubDirRelativePath";

        private HierarchyDirectoryTransfer rootDirectoryTransfer = null;
        private ITransferEnumerator transferEnumerator = null;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private string relativePath = null;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableListContinuationToken enumerateContinuationToken = null;

        private TransferLocation source;
        private TransferLocation dest;

        public SubDirectoryTransfer(
            HierarchyDirectoryTransfer rootDirectoryTransfer,
            string relativePath)
        {
            this.enumerateContinuationToken = new SerializableListContinuationToken(null);
            this.rootDirectoryTransfer = rootDirectoryTransfer;
            this.relativePath = relativePath;
            this.rootDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
            this.InitializeEnumerator();
        }

        public SubDirectoryTransfer(SubDirectoryTransfer other)
        {
            this.relativePath = other.relativePath;
            this.enumerateContinuationToken = other.enumerateContinuationToken;
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SubDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected SubDirectoryTransfer(SerializationInfo info, StreamingContext context)
        {
            this.relativePath = info.GetString(SubDirRelativePathName);

            if (!(context.Context is StreamJournal))
            {
                this.enumerateContinuationToken = (SerializableListContinuationToken)info.GetValue(SubDirListContinuationTokenName, typeof(SerializableListContinuationToken));
            }
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(SubDirRelativePathName, this.relativePath, typeof(string));

            if (!(context.Context is StreamJournal))
            {
                // serialize continuation token
                info.AddValue(SubDirListContinuationTokenName, this.enumerateContinuationToken, typeof(SerializableListContinuationToken));
            }
        }
#endif // BINARY_SERIALIZATION

        public SerializableListContinuationToken ListContinuationToken
        {
            get
            {
                return this.enumerateContinuationToken;
            }

            set
            {
                this.enumerateContinuationToken = value;
            }
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();

            this.CreateDestinationDirectory(cancellationToken);

            var enumerator = this.transferEnumerator.EnumerateLocation(cancellationToken).GetEnumerator();

            while (true)
            {
                if (!enumerator.MoveNext())
                {
                    break;
                }

                TransferEntry entry = enumerator.Current;
                ErrorEntry errorEntry = entry as ErrorEntry;
                if (errorEntry != null)
                {
                    TransferException exception = errorEntry.Exception as TransferException;
                    if (null != exception)
                    {
                        throw exception;
                    }
                    else
                    {
                        throw new TransferException(
                            TransferErrorCode.FailToEnumerateDirectory,
                            errorEntry.Exception.GetExceptionMessage(),
                            errorEntry.Exception);
                    }
                }

                if (entry.IsDirectory)
                {
                    this.rootDirectoryTransfer.AddSubDir(entry.RelativePath, () =>
                    {
                        var currentContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                        currentContinuationToken.Journal = this.enumerateContinuationToken.Journal;
                        currentContinuationToken.StreamJournalOffset = this.enumerateContinuationToken.StreamJournalOffset;
                        this.enumerateContinuationToken = currentContinuationToken;
                        return this.enumerateContinuationToken;
                    });
                }
                else
                {
                    SingleObjectTransfer transferItem = this.rootDirectoryTransfer.CreateTransfer(entry);
#if DEBUG
                    Utils.HandleFaultInjection(entry.RelativePath, transferItem);
#endif

                    this.CreateDestinationParentDirectoryRecursively(transferItem);

                    this.rootDirectoryTransfer.AddSingleObjectTransfer(transferItem, () =>
                    {
                        var currentContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                        currentContinuationToken.Journal = this.enumerateContinuationToken.Journal;
                        currentContinuationToken.StreamJournalOffset = this.enumerateContinuationToken.StreamJournalOffset;
                        this.enumerateContinuationToken = currentContinuationToken;
                        return this.enumerateContinuationToken;
                    });
                }
            }
        }

        public void Update(HierarchyDirectoryTransfer rootDirectoryTransferInstance)
        {
            this.rootDirectoryTransfer = rootDirectoryTransferInstance;
            this.rootDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
            this.InitializeEnumerator();
        }

        public void CreateDestinationParentDirectoryRecursively(SingleObjectTransfer transferItem)
        {
            switch (transferItem.Destination.Type)
            {
                case TransferLocationType.FilePath:
                    var filePath = (transferItem.Destination as FileLocation).FilePath;
                    var currentDir = this.dest.Instance as string;
                    Utils.ValidateDestinationPath(transferItem.Source.Instance.ConvertToString(), filePath);
                    Utils.CreateParentDirectoryIfNotExists(filePath);
                    break;
                case TransferLocationType.AzureFile:
                    var parent = (transferItem.Destination as AzureFileLocation).AzureFile.Parent;
                    CloudFileDirectory destDirectory = this.dest.Instance as CloudFileDirectory;

                    if (!string.Equals(parent.SnapshotQualifiedUri.AbsolutePath, destDirectory.SnapshotQualifiedUri.AbsolutePath))
                    {
                        if (this.rootDirectoryTransfer.IsForceOverwrite || !parent.ExistsAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).Result)
                        {
                            Utils.CreateCloudFileDirectoryRecursively(parent);
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        private void InitializeEnumerator()
        {
            if (this.source.Type == TransferLocationType.AzureFileDirectory)
            {
                var fileEnumerator = new AzureFileHierarchyEnumerator(this.source as AzureFileDirectoryLocation, rootDirectoryTransfer.Source.Instance as CloudFileDirectory);
                fileEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
                fileEnumerator.SearchPattern = this.rootDirectoryTransfer.SearchPattern;
                fileEnumerator.Recursive = this.rootDirectoryTransfer.Recursive;

                this.transferEnumerator = fileEnumerator;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        private void CreateDestinationDirectory(CancellationToken cancellationToken)
        {
            if (this.dest.Type == TransferLocationType.LocalDirectory)
            {
                var localFileDestLocation = this.dest as DirectoryLocation;
                if (!LongPathDirectory.Exists(localFileDestLocation.DirectoryPath))
                {
                    LongPathDirectory.CreateDirectory(localFileDestLocation.DirectoryPath);
                }
            }
            else if (this.dest.Type == TransferLocationType.AzureFileDirectory)
            {
                AzureFileDirectoryLocation fileDirLocation = this.dest as AzureFileDirectoryLocation;

                var fileDirectory = fileDirLocation.FileDirectory;

                if (string.Equals(fileDirectory.SnapshotQualifiedUri.AbsolutePath, fileDirectory.Share.SnapshotQualifiedUri.AbsolutePath))
                {
                    return;
                }

                try
                {
                    fileDirLocation.FileDirectory.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null, cancellationToken).GetAwaiter().GetResult();
                }
                catch (StorageException ex)
                {
                    if (null == ex.RequestInformation || !string.Equals("ResourceAlreadyExists", ex.RequestInformation.ErrorCode))
                    {
                        throw;
                    }
                }
            }
        }
    }
}
