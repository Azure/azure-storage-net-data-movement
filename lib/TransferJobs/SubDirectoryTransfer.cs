//------------------------------------------------------------------------------
// <copyright file="SubDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.Azure.Storage.File;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    class SubDirectoryTransfer : Transfer
    {
        private const string SubDirListContinuationTokenName = "SubDirListContinuationToken";

        private HierarchyDirectoryTransfer rootDirectoryTransfer = null;
        private ITransferEnumerator transferEnumerator = null;

        private SerializableListContinuationToken enumerateContinuationToken = null;

        public SubDirectoryTransfer(
            TransferLocation subDirSourceLocation,
            TransferLocation dest,
            TransferMethod transferMethod,
            HierarchyDirectoryTransfer rootDirectoryTransfer)
            : base(subDirSourceLocation, dest, transferMethod)
        {
            this.enumerateContinuationToken = new SerializableListContinuationToken(null);
            this.rootDirectoryTransfer = rootDirectoryTransfer;
            this.InitializeEnumerator();
        }

        public SubDirectoryTransfer(SubDirectoryTransfer other)
            : base(other)
        {
            this.enumerateContinuationToken = other.enumerateContinuationToken;
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SubDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected SubDirectoryTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
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
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (!(context.Context is StreamJournal))
            {
                // serialize enumerator
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

        public override async Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
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

        // This method should never be called.
        public override Transfer Copy()
        {
            throw new NotSupportedException();
        }

        public void Update(HierarchyDirectoryTransfer rootDirectoryTransferInstance)
        {
            this.rootDirectoryTransfer = rootDirectoryTransferInstance;

            if (this.Source.Type == TransferLocationType.AzureBlobDirectory)
            {
                var rootSourceLocation = this.rootDirectoryTransfer.Source as AzureBlobDirectoryLocation;
                var subDirSourceLocation = this.Source as AzureBlobDirectoryLocation;

                subDirSourceLocation.UpdateCredentials(rootSourceLocation.BlobDirectory.ServiceClient.Credentials);
            }
            else if (this.Source.Type == TransferLocationType.AzureFileDirectory)
            {
                var rootSourceLocation = this.rootDirectoryTransfer.Source as AzureFileDirectoryLocation;
                var subDirSourceLocation = this.Source as AzureFileDirectoryLocation;

                subDirSourceLocation.UpdateCredentials(rootSourceLocation.FileDirectory.ServiceClient.Credentials);
            }

            if (this.Destination.Type == TransferLocationType.AzureBlobDirectory)
            {
                var rootDestLocation = this.rootDirectoryTransfer.Destination as AzureBlobDirectoryLocation;
                var subDirDestLocation = this.Destination as AzureBlobDirectoryLocation;

                subDirDestLocation.UpdateCredentials(rootDestLocation.BlobDirectory.ServiceClient.Credentials);
            }
            else if (this.Destination.Type == TransferLocationType.AzureFileDirectory)
            {
                var rootDestLocation = this.rootDirectoryTransfer.Destination as AzureFileDirectoryLocation;
                var subDirDestLocation = this.Destination as AzureFileDirectoryLocation;

                subDirDestLocation.UpdateCredentials(rootDestLocation.FileDirectory.ServiceClient.Credentials);
            }

            this.InitializeEnumerator();
        }

        private void InitializeEnumerator()
        {
            if (this.Source.Type == TransferLocationType.AzureFileDirectory)
            {
                var fileEnumerator = new AzureFileHierarchyEnumerator(this.Source as AzureFileDirectoryLocation, rootDirectoryTransfer.Source.Instance as CloudFileDirectory);
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
            if (this.Destination.Type == TransferLocationType.LocalDirectory)
            {
                var localFileDestLocation = this.Destination as DirectoryLocation;
                if (!LongPathDirectory.Exists(localFileDestLocation.DirectoryPath))
                {
                    LongPathDirectory.CreateDirectory(localFileDestLocation.DirectoryPath);
                }
            }
            else if (this.Destination.Type == TransferLocationType.AzureFileDirectory)
            {
                AzureFileDirectoryLocation fileDirLocation = this.Destination as AzureFileDirectoryLocation;

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
