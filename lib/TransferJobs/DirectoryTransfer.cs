﻿//------------------------------------------------------------------------------
// <copyright file="DirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Net;
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
        /// These filenames are reserved on windows, regardless of the file extension.
        /// </summary>
        private static readonly string[] ReservedBaseFileNames = new string[]
            {
                "CON", "PRN", "AUX", "NUL",
                "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
                "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
            };

        /// <summary>
        /// These filenames are reserved on windows, only if the full filename matches.
        /// </summary>
        private static readonly string[] ReservedFileNames = new string[]
            {
                "CLOCK$",
            };

        /// <summary>
        /// Serialization field name for bool to indicate whether delimiter is set.
        /// </summary>
        private const string HasDelimiterName = "HasDelimiter";

        /// <summary>
        /// Serialization field name for delimiter.
        /// </summary>
        private const string DelimiterName = "Delimiter";

        /// <summary>
        /// Name resolver.
        /// </summary>
        private INameResolver nameResolver;

        /// <summary>
        /// Records last Azure file directory created to optimize Azure file directory check.
        /// </summary>
        private CloudFileDirectory lastAzureFileDirectory = null;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif // BINARY_SERIALIZATION
        private char? delimiter = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public DirectoryTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
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
            if ((bool)info.GetValue(HasDelimiterName, typeof(bool)))
            {
                this.delimiter = (char)info.GetValue(DelimiterName, typeof(char));
            }
        }
#endif // BINARY_SERIALIZATION

        // Initialize a new DirectoryTransfer object after deserialization
#if !BINARY_SERIALIZATION
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="DirectoryTransfer"/> object.</param>
        private DirectoryTransfer(DirectoryTransfer other)
            : base(other)
        {
        }

        public char? Delimiter
        {
            get
            {
                return this.delimiter;
            }

            set
            {
                this.delimiter = value;
            }
        }

        public bool IsForceOverwrite
        {
            get
            {
                if (this.DirectoryContext == null)
                {
                    return false;
                }

                return this.DirectoryContext.ShouldOverwriteCallbackAsync == TransferContext.ForceOverwrite;
            }
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

            // serialize sub transfers
            info.AddValue(HasDelimiterName, this.delimiter.HasValue, typeof(bool));

            if (this.delimiter.HasValue)
            {
                info.AddValue(DelimiterName, this.delimiter.Value);
            }
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Creates a copy of current transfer object.
        /// </summary>
        /// <returns>A copy of current transfer object.</returns>
        public override Transfer Copy()
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
            catch (StorageException se)
            {
                throw new TransferException(TransferErrorCode.FailToVadlidateDestination,
                    Resources.FailedToValidateDestinationException,
                    se);
            }
            catch (Exception ex)
            {
                throw new TransferException(TransferErrorCode.FailToVadlidateDestination,
                    Resources.FailedToValidateDestinationException,
                    ex);
            }

            this.nameResolver = GetNameResolver(this.Source, this.Destination, this.Delimiter);
            await base.ExecuteAsync(scheduler, cancellationToken);
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

                    return new FileLocation(fileEntry.FullPath, fileEntry.RelativePath);
                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        private TransferLocation GetDestinationTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            string destRelativePath = this.nameResolver.ResolveName(entry);

            AzureBlobEntry sourceBlobEntry = entry as AzureBlobEntry;

            switch (dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    {
                        AzureBlobDirectoryLocation blobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                        BlobType destBlobType = this.BlobType;

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

                        AzureFileLocation retLocation = new AzureFileLocation(file);
                        retLocation.FileRequestOptions = fileDirLocation.FileRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.LocalDirectory:
                    {
                        DirectoryLocation localDirLocation = dirLocation as DirectoryLocation;
                        string path = Path.Combine(localDirLocation.DirectoryPath, destRelativePath);
                        
                        return new FileLocation(path, destRelativePath);
                    }

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        protected override void CreateParentDirectory(SingleObjectTransfer transfer)
        {
            switch (transfer.Destination.Type)
            {
                case TransferLocationType.FilePath:
                    var filePath = (transfer.Destination as FileLocation).FilePath;
                    ValidateDestinationPath(transfer.Source.Instance.ConvertToString(), filePath);
                    CreateParentDirectoryIfNotExists(filePath);
                    break;
                case TransferLocationType.AzureFile:
                    try
                    {
                        CreateParentDirectoryIfNotExists((transfer.Destination as AzureFileLocation).AzureFile);
                    }
                    catch (Exception ex)
                    {
                        AggregateException aggregateException = ex as AggregateException;
                        StorageException storageException = null;
                        if (aggregateException != null)
                        {
                            storageException = aggregateException.Flatten().InnerExceptions[0] as StorageException;
                        }

                        if (storageException == null)
                        {
                            storageException = ex as StorageException;
                        }

                        if (storageException != null)
                        {
                            throw new TransferException(TransferErrorCode.FailToVadlidateDestination, 
                                string.Format(CultureInfo.CurrentCulture,
                                    Resources.FailedToValidateDestinationException,
                                    storageException.ToErrorDetail()),
                                storageException);
                        }

                        throw;
                    }
                    break;
                default:
                    break;
            }
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object will be disposed by the caller")]
        protected override SingleObjectTransfer CreateTransfer(TransferEntry entry)
        {
            TransferLocation sourceLocation = GetSourceTransferLocation(this.Source, entry);
            sourceLocation.IsInstanceInfoFetched = true;
            TransferLocation destLocation = GetDestinationTransferLocation(this.Destination, entry);
            var transferMethod = IsDummyCopy(entry) ? TransferMethod.DummyCopy : this.TransferMethod;
            SingleObjectTransfer transfer = new SingleObjectTransfer(sourceLocation, destLocation, transferMethod);
            transfer.Context = this.Context;
            return transfer;
        }

        private bool IsDummyCopy(TransferEntry entry)
        {
            if (this.Source.Type == TransferLocationType.AzureBlobDirectory
                && this.Destination.Type == TransferLocationType.LocalDirectory)
            {
                if(IsDirectoryBlob((entry as AzureBlobEntry)?.Blob))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsDirectoryBlob(CloudBlob blob)
        {
            if (blob != null)
            {
                if (blob.Properties.Length == 0)
                {
                    foreach (var metadata in blob.Metadata)
                    {
                        if (String.Compare(metadata.Key, Constants.DirectoryBlobMetadataKey, StringComparison.OrdinalIgnoreCase) == 0
                            && String.Compare(metadata.Value, "true", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
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

        private static INameResolver GetNameResolver(TransferLocation sourceLocation, TransferLocation destLocation, char? delimiter)
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
                        return new AzureBlobToAzureFileNameResolver(delimiter);
                    }
                    else if(destLocation.Type == TransferLocationType.LocalDirectory)
                    {
                        return new AzureToFileNameResolver(delimiter);
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

        private static void ValidateDestinationPath(string sourcePath, string destPath)
        {
            if (Interop.CrossPlatformHelpers.IsWindows)
            {
                if (!IsValidWindowsFileName(destPath))
                {
                    throw new TransferException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.SourceNameInvalidInFileSystem,
                            sourcePath));
                }
            }
        }

        private static bool IsValidWindowsFileName(string fileName)
        {
            string fileNameNoExt = LongPath.GetFileNameWithoutExtension(fileName);
            string fileNameWithExt = LongPath.GetFileName(fileName);

            if (Array.Exists<string>(ReservedBaseFileNames, delegate (string s) { return fileNameNoExt.Equals(s, StringComparison.OrdinalIgnoreCase); }))
            {
                return false;
            }

            if (Array.Exists<string>(ReservedFileNames, delegate (string s) { return fileNameWithExt.Equals(s, StringComparison.OrdinalIgnoreCase); }))
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(fileNameWithExt))
            {
                return false;
            }

            bool allDotsOrWhiteSpace = true;
            for (int i = 0; i < fileName.Length; ++i)
            {
                if (fileName[i] != '.' && !char.IsWhiteSpace(fileName[i]))
                {
                    allDotsOrWhiteSpace = false;
                    break;
                }
            }

            if (allDotsOrWhiteSpace)
            {
                return false;
            }

            return true;
        }

        private void CreateParentDirectoryIfNotExists(CloudFile file)
        {
            FileRequestOptions fileRequestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
            CloudFileDirectory parent = file.Parent;
            if (!this.IsLastDirEqualsOrSubDirOf(parent))
            {
                if (this.IsForceOverwrite || !parent.ExistsAsync(fileRequestOptions, null).Result)
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
                string absoluteUri1 = AppendSlash(this.lastAzureFileDirectory.SnapshotQualifiedUri.AbsoluteUri);
                string absoluteUri2 = AppendSlash(dir.SnapshotQualifiedUri.AbsoluteUri);

                if (absoluteUri1.StartsWith(absoluteUri2, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        private static void CreateParentDirectoryIfNotExists(string path)
        {
            string dir = LongPath.GetDirectoryName(path);

            if (!string.IsNullOrEmpty(dir) && !LongPathDirectory.Exists(dir))
            {
                LongPathDirectory.CreateDirectory(dir);
            }
        }

        private void CreateCloudFileDirectoryRecursively(CloudFileDirectory dir)
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

                try
                {
                    // create anyway, ignore 409 and 403
                    dir.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).Wait();
                }
                catch (AggregateException e)
                {
                    StorageException innnerException = e.Flatten().InnerExceptions[0] as StorageException;
                    if (!IgnoreDirectoryCreationError(innnerException))
                    {
                        throw;
                    }
                }
            }
        }

        private static bool IgnoreDirectoryCreationError(StorageException se)
        {
            if (null == se)
            {
                return false;
            }

            if (Utils.IsExpectedHttpStatusCodes(se, HttpStatusCode.Forbidden))
            {
                return true;
            }

            if (null != se.RequestInformation
                && se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict
                && string.Equals(se.RequestInformation.ErrorCode, "ResourceAlreadyExists"))
            {
                return true;
            }

            return false;
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
