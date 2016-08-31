//------------------------------------------------------------------------------
// <copyright file="DirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Represents a directory object transfer operation.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class DirectoryTransfer : MultipleObjectsTransfer
    {
        /// <summary>
        /// Name resolver.
        /// </summary>
        private INameResolver nameResolver;

        /// <summary>
        /// Records last Azure file directory created to optimize Azure file directory check.
        /// </summary>
        private CloudFileDirectory lastAzureFileDirectory = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public DirectoryTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
            this.Initialize();
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected DirectoryTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.Initialize();
        }
#endif // BINARY_SERIALIZATION

        // Initialize a new DirectoryTransfer object after deserialization
#if !BINARY_SERIALIZATION
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            this.Initialize();
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="DirectoryTransfer"/> object.</param>
        private DirectoryTransfer(DirectoryTransfer other)
            : base(other)
        {
            this.Initialize();
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Creates a copy of current transfer object.
        /// </summary>
        /// <returns>A copy of current transfer object.</returns>
        public DirectoryTransfer Copy()
        {
            return new DirectoryTransfer(this);
        }

        /// <summary>
        /// Execute the transfer asynchronously.
        /// </summary>
        /// <param name="scheduler">Transfer scheduler</param>
        /// <param name="cancellationToken">Token that can be used to cancel the transfer.</param>
        /// <returns>A task representing the transfer operation.</returns>
        public override async Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            try
            {
                this.Destination.Validate();
            }
            catch(Exception ex)
            {
                throw new TransferException(TransferErrorCode.FailToVadlidateDestination, Resources.FailedToValidateDestinationException, ex);
            }

            await base.ExecuteAsync(scheduler, cancellationToken);
        }

        private void Initialize()
        {
            this.nameResolver = GetNameResolver(this.Source, this.Destination);
        }

        private static TransferLocation GetSourceTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            switch(dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    AzureBlobDirectoryLocation azureBlobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                    AzureBlobEntry azureBlobEntry = entry as AzureBlobEntry;

                    AzureBlobLocation azureBlobLocation = new AzureBlobLocation(azureBlobEntry.Blob);
                    azureBlobLocation.BlobRequestOptions = azureBlobDirLocation.BlobRequestOptions;

                    return azureBlobLocation;
                case TransferLocationType.AzureFileDirectory:
                    AzureFileDirectoryLocation azureFileDirLocation = dirLocation as AzureFileDirectoryLocation;
                    AzureFileEntry azureFileEntry = entry as AzureFileEntry;

                    AzureFileLocation azureFileLocation = new AzureFileLocation(azureFileEntry.File);
                    azureFileLocation.FileRequestOptions = azureFileDirLocation.FileRequestOptions;

                    return azureFileLocation;
                case TransferLocationType.LocalDirectory:
                    FileEntry fileEntry = entry as FileEntry;

                    return new FileLocation(fileEntry.FullPath);
                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        private TransferLocation GetDestinationTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            string destRelativePath = this.nameResolver.ResolveName(entry);

            switch(dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    {
                        AzureBlobDirectoryLocation blobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                        BlobType destBlobType = this.BlobType;

                        AzureBlobEntry sourceBlobEntry = entry as AzureBlobEntry;
                        if (sourceBlobEntry != null)
                        {
                            // if source is Azure blob storage, source and destination blob share the same blob type
                            destBlobType = sourceBlobEntry.Blob.BlobType;
                        }

                        CloudBlob blob = null;
                        switch (destBlobType)
                        {
                            case Blob.BlobType.BlockBlob:
                            case Blob.BlobType.Unspecified:
                                blob = blobDirLocation.BlobDirectory.GetBlockBlobReference(destRelativePath);
                                break;

                            case Blob.BlobType.PageBlob:
                                blob = blobDirLocation.BlobDirectory.GetPageBlobReference(destRelativePath);
                                break;

                            case Blob.BlobType.AppendBlob:
                                blob = blobDirLocation.BlobDirectory.GetAppendBlobReference(destRelativePath);
                                break;
                        }

                        AzureBlobLocation retLocation = new AzureBlobLocation(blob);
                        retLocation.BlobRequestOptions = blobDirLocation.BlobRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.AzureFileDirectory:
                    {
                        AzureFileDirectoryLocation fileDirLocation = dirLocation as AzureFileDirectoryLocation;
                        CloudFile file = fileDirLocation.FileDirectory.GetFileReference(destRelativePath);
                        CreateParentDirectoryIfNotExists(file);

                        AzureFileLocation retLocation = new AzureFileLocation(file);
                        retLocation.FileRequestOptions = fileDirLocation.FileRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.LocalDirectory:
                    {
                        DirectoryLocation localDirLocation = dirLocation as DirectoryLocation;
                        string path = Path.Combine(localDirLocation.DirectoryPath, destRelativePath);
                        CreateParentDirectoryIfNotExists(path);

                        return new FileLocation(path);
                    }

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object will be disposed by the caller")]
        protected override SingleObjectTransfer CreateTransfer(TransferEntry entry)
        {
            TransferLocation sourceLocation = GetSourceTransferLocation(this.Source, entry);
            TransferLocation destLocation = GetDestinationTransferLocation(this.Destination, entry);
            SingleObjectTransfer transfer = new SingleObjectTransfer(sourceLocation, destLocation, this.TransferMethod);
            transfer.Context = this.Context;
            transfer.ContentType = this.ContentType;
            return transfer;
        }

        protected override void UpdateTransfer(Transfer transfer)
        {
            DirectoryTransfer.UpdateCredentials(this.Source, transfer.Source);
            DirectoryTransfer.UpdateCredentials(this.Destination, transfer.Destination);
        }

        private static void UpdateCredentials(TransferLocation dirLocation, TransferLocation subLocation)
        {
            if (dirLocation.Type == TransferLocationType.AzureBlobDirectory)
            {
                AzureBlobDirectoryLocation blobDirectoryLocation = dirLocation as AzureBlobDirectoryLocation;
                (subLocation as AzureBlobLocation).UpdateCredentials(blobDirectoryLocation.BlobDirectory.ServiceClient.Credentials);
            }
            else if (dirLocation.Type == TransferLocationType.AzureFileDirectory)
            {
                AzureFileDirectoryLocation fileDirectoryLocation = dirLocation as AzureFileDirectoryLocation;
                (subLocation as AzureFileLocation).UpdateCredentials(fileDirectoryLocation.FileDirectory.ServiceClient.Credentials);
            }
        }

        private static INameResolver GetNameResolver(TransferLocation sourceLocation, TransferLocation destLocation)
        {
            Debug.Assert(sourceLocation != null, "sourceLocation");
            Debug.Assert(destLocation != null, "destLocation");

            switch(sourceLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    if (destLocation.Type == TransferLocationType.AzureBlobDirectory)
                    {
                        return new AzureBlobToAzureBlobNameResolver();
                    }
                    else if(destLocation.Type == TransferLocationType.AzureFileDirectory)
                    {
                        return new AzureBlobToAzureFileNameResolver(null);
                    }
                    else if(destLocation.Type == TransferLocationType.LocalDirectory)
                    {
                        return new AzureToFileNameResolver(null);
                    }
                    break;

                case TransferLocationType.AzureFileDirectory:
                    if (destLocation.Type == TransferLocationType.AzureBlobDirectory ||
                        destLocation.Type == TransferLocationType.AzureFileDirectory)
                    {
                        return new AzureFileToAzureNameResolver();
                    }
                    else if(destLocation.Type == TransferLocationType.LocalDirectory)
                    {
                        return new AzureToFileNameResolver(null);
                    }
                    break;

                case TransferLocationType.LocalDirectory:
                    if (destLocation.Type == TransferLocationType.AzureBlobDirectory)
                    {
                        return new FileToAzureBlobNameResolver();
                    }
                    else if (destLocation.Type == TransferLocationType.AzureFileDirectory)
                    {
                        return new FileToAzureFileNameResolver();
                    }
                    break;

                default:
                    throw new ArgumentException("Unsupported source location", "sourceLocation");
            }

            throw new ArgumentException("Unsupported destination location", "destLocation");
        }

        private void CreateParentDirectoryIfNotExists(CloudFile file)
        {
            FileRequestOptions fileRequestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
            CloudFileDirectory parent = file.Parent;
            if (!this.IsLastDirEqualsOrSubDirOf(parent))
            {
                if (!parent.ExistsAsync(fileRequestOptions, null).Result)
                {
                    CreateCloudFileDirectoryRecursively(parent);
                }

                this.lastAzureFileDirectory = parent;
            }
        }

        private bool IsLastDirEqualsOrSubDirOf(CloudFileDirectory dir)
        {
            if (null == dir)
            {
                // Both null, equals
                return null == this.lastAzureFileDirectory;
            }
            else if (null != this.lastAzureFileDirectory)
            {
                string absoluteUri1 = AppendSlash(this.lastAzureFileDirectory.Uri.AbsoluteUri);
                string absoluteUri2 = AppendSlash(dir.Uri.AbsoluteUri);

                if (absoluteUri1.StartsWith(absoluteUri2, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        private static void CreateParentDirectoryIfNotExists(string path)
        {
            string dir = Path.GetDirectoryName(path);

            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
        }

        private static void CreateCloudFileDirectoryRecursively(CloudFileDirectory dir)
        {
            if (null == dir)
            {
                return;
            }

            CloudFileDirectory parent = dir.Parent;

            // null == parent means dir is root directory, 
            // we should not call CreateIfNotExists in that case
            if (null != parent)
            {
                CreateCloudFileDirectoryRecursively(parent);
                dir.CreateIfNotExistsAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).Wait();
            }
        }

        private static string AppendSlash(string input)
        {
            if (input.EndsWith("/", StringComparison.Ordinal))
            {
                return input;
            }
            else
            {
                return input + "/";
            }
        }
    }
}
