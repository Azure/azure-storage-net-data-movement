//------------------------------------------------------------------------------
// <copyright file="SubDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Represents a sub-directory transfer under a hierarchy directory transfer.
    /// </summary>
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

        /// <summary>
        /// Base <see cref="HierarchyDirectoryTransfer"/> instance which this <see cref="SubDirectoryTransfer"/> instance belongs to.
        /// <see cref="SubDirectoryTransfer"/> instance returns its listed directories and files to <see cref="HierarchyDirectoryTransfer"/> with callbacks.
        /// </summary>
        private HierarchyDirectoryTransfer baseDirectoryTransfer = null;
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
            HierarchyDirectoryTransfer baseDirectoryTransfer,
            string relativePath)
        {
            this.enumerateContinuationToken = new SerializableListContinuationToken(null);
            this.baseDirectoryTransfer = baseDirectoryTransfer;
            this.relativePath = relativePath;
            this.baseDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
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
                    this.baseDirectoryTransfer.AddSubDir(entry.RelativePath, () =>
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
                    SingleObjectTransfer transferItem = this.baseDirectoryTransfer.CreateTransfer(entry);
                    transferItem.SDDLCache = this.baseDirectoryTransfer.SDDLCache;
#if DEBUG
                    Utils.HandleFaultInjection(entry.RelativePath, transferItem);
#endif

                    this.CreateDestinationParentDirectoryRecursively(transferItem);

                    this.baseDirectoryTransfer.AddSingleObjectTransfer(transferItem, () =>
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

        public void Update(HierarchyDirectoryTransfer baseDirectoryTransferInstance)
        {
            this.baseDirectoryTransfer = baseDirectoryTransferInstance;
            this.baseDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
            this.InitializeEnumerator();
        }

        public void CreateDestinationParentDirectoryRecursively(SingleObjectTransfer transferItem)
        {
            switch (transferItem.Destination.Type)
            {
                case TransferLocationType.FilePath:
                    var filePath = (transferItem.Destination as FileLocation).FilePath;
                    Utils.ValidateDestinationPath(transferItem.Source.Instance.ConvertToString(), filePath);
                    Utils.CreateParentDirectoryIfNotExists(filePath);
                    break;
                case TransferLocationType.AzureFile:
                    var parent = (transferItem.Destination as AzureFileLocation).AzureFile.Parent;
                    CloudFileDirectory destDirectory = this.dest.Instance as CloudFileDirectory;

                    if (!string.Equals(parent.SnapshotQualifiedUri.AbsolutePath, destDirectory.SnapshotQualifiedUri.AbsolutePath))
                    {
                        if (this.baseDirectoryTransfer.IsForceOverwrite || !parent.ExistsAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).Result)
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
                var fileEnumerator = new AzureFileHierarchyEnumerator(this.source as AzureFileDirectoryLocation, this.baseDirectoryTransfer.Source.Instance as CloudFileDirectory);
                fileEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
                fileEnumerator.SearchPattern = this.baseDirectoryTransfer.SearchPattern;
                fileEnumerator.Recursive = this.baseDirectoryTransfer.Recursive;

                this.transferEnumerator = fileEnumerator;
            }
            else if (this.source.Type == TransferLocationType.LocalDirectory)
            {
                var fileEnumerator = new FileHierarchyEnumerator(this.source as DirectoryLocation, this.baseDirectoryTransfer.Source.Instance as string, this.baseDirectoryTransfer.FollowSymblink);
                fileEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
                fileEnumerator.SearchPattern = this.baseDirectoryTransfer.SearchPattern;
                fileEnumerator.Recursive = this.baseDirectoryTransfer.Recursive;

                this.transferEnumerator = fileEnumerator;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        private void CreateDestinationDirectory(CancellationToken cancellationToken)
        {
            if (this.dest.Type == TransferLocationType.AzureBlobDirectory)
            {
                // No physical destination directory needed.
                return;
            }

            if ((this.dest.Type == TransferLocationType.AzureFileDirectory)
                && (null == (this.dest as AzureFileDirectoryLocation).FileDirectory.Parent))
            {
                //Root Azure File directory, no need to create.
                return;
            }

            CloudFileNtfsAttributes? fileAttributes = null;
            string portableSDDL = null;
            DateTimeOffset? creationTime = null;
            DateTimeOffset? lastWriteTime = null;
            IDictionary<string, string> metadata = null;

            this.GetSourceProperites(out fileAttributes, out portableSDDL, out creationTime, out lastWriteTime, out metadata, cancellationToken);

            if (this.dest.Type == TransferLocationType.LocalDirectory)
            {
                var localFileDestLocation = this.dest as DirectoryLocation;
                if (!LongPathDirectory.Exists(localFileDestLocation.DirectoryPath))
                {
                    LongPathDirectory.CreateDirectory(localFileDestLocation.DirectoryPath);
                }

                if (fileAttributes.HasValue)
                {
                    LongPathFile.SetFileTime(localFileDestLocation.DirectoryPath, creationTime.Value, lastWriteTime.Value, true);
                    LongPathFile.SetAttributes(localFileDestLocation.DirectoryPath, Utils.AzureFileNtfsAttributesToLocalAttributes(fileAttributes.Value));
                }

                if (!string.IsNullOrEmpty(portableSDDL))
                {
                    FileSecurityOperations.SetFileSecurity(localFileDestLocation.DirectoryPath, portableSDDL, this.baseDirectoryTransfer.PreserveSMBPermissions);
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
                    CreateCloudFileDestinationDirectory(fileDirectory, 
                        fileAttributes,
                        creationTime,
                        lastWriteTime,
                        metadata,
                        portableSDDL,
                        cancellationToken);
                }
                catch (StorageException storageException)
                {
                    if ((null != storageException.InnerException)
                        && (storageException.InnerException is OperationCanceledException))
                    {
                        throw storageException.InnerException;
                    }
                    else
                    {
                        throw new TransferException(TransferErrorCode.FailToVadlidateDestination,
                            string.Format(CultureInfo.CurrentCulture,
                                Resources.FailedToValidateDestinationException,
                                storageException.ToErrorDetail()),
                            storageException);
                    }
                }
            }
        }

        private void GetSourceProperites(out CloudFileNtfsAttributes? fileAttributes,
            out string portableSDDL,
            out DateTimeOffset? creationTime,
            out DateTimeOffset? lastWriteTime,
            out IDictionary<string, string> metadata,
            CancellationToken cancellationToken)
        {
            fileAttributes = null;
            portableSDDL = null;
            creationTime = null;
            lastWriteTime = null;
            metadata = null;

            if (this.source.Type == TransferLocationType.LocalDirectory)
            {
                if ((this.baseDirectoryTransfer.PreserveSMBAttributes)
                    || (PreserveSMBPermissions.None != this.baseDirectoryTransfer.PreserveSMBPermissions))
                {
                    var sourceLocalDirLocation = this.source as DirectoryLocation;
                    string directoryPath = sourceLocalDirLocation.DirectoryPath;

                    if (this.baseDirectoryTransfer.PreserveSMBAttributes)
                    {
                        FileAttributes? localFileAttributes = null;
#if DOTNET5_4
                        LongPathFile.GetFileProperties(directoryPath, out creationTime, out lastWriteTime, out localFileAttributes, true);
#else
                        LongPathFile.GetFileProperties(directoryPath, out creationTime, out lastWriteTime, out localFileAttributes);
#endif
                        fileAttributes = Utils.LocalAttributesToAzureFileNtfsAttributes(localFileAttributes.Value);
                    }

                    portableSDDL = FileSecurityOperations.GetFilePortableSDDL(directoryPath, this.baseDirectoryTransfer.PreserveSMBPermissions);
                }


            }
            else if (this.source.Type == TransferLocationType.AzureFileDirectory)
            {
                if (this.baseDirectoryTransfer.PreserveSMBAttributes
                    || (PreserveSMBPermissions.None != this.baseDirectoryTransfer.PreserveSMBPermissions)
                    || this.dest.Type == TransferLocationType.AzureFileDirectory)
                {
                    AzureFileDirectoryLocation sourceFileDirLocation = this.source as AzureFileDirectoryLocation;

                    var sourceFileDirectory = sourceFileDirLocation.FileDirectory;
                    sourceFileDirectory.FetchAttributesAsync(
                        null,
                        Utils.GenerateFileRequestOptions(sourceFileDirLocation.FileRequestOptions),
                        Utils.GenerateOperationContext(this.baseDirectoryTransfer.Context),
                        cancellationToken).GetAwaiter().GetResult();

                    if (this.baseDirectoryTransfer.PreserveSMBAttributes)
                    {
                        fileAttributes = sourceFileDirectory.Properties.NtfsAttributes;
                        creationTime = sourceFileDirectory.Properties.CreationTime;
                        lastWriteTime = sourceFileDirectory.Properties.LastWriteTime;
                    }

                    metadata = sourceFileDirectory.Metadata;

                    if (PreserveSMBPermissions.None != this.baseDirectoryTransfer.PreserveSMBPermissions)
                    {
                        if (!string.IsNullOrEmpty(sourceFileDirectory.FilePermission))
                        {
                            portableSDDL = sourceFileDirectory.FilePermission;
                        }
                        else if (!string.IsNullOrEmpty(sourceFileDirectory.Properties.FilePermissionKey))
                        {
                            portableSDDL = this.baseDirectoryTransfer.SDDLCache.GetValue(
                                sourceFileDirectory.Properties.FilePermissionKey);

                            if (null == portableSDDL)
                            {
                                portableSDDL = sourceFileDirectory.Share.GetFilePermissionAsync(
                                    sourceFileDirectory.Properties.FilePermissionKey,
                                    Utils.GenerateFileRequestOptions(sourceFileDirLocation.FileRequestOptions),
                                    Utils.GenerateOperationContext(this.baseDirectoryTransfer.Context),
                                    cancellationToken).GetAwaiter().GetResult();
                                this.baseDirectoryTransfer.SDDLCache.AddValue(sourceFileDirectory.Properties.FilePermissionKey,
                                    portableSDDL);
                            }
                        }
                        else
                        {
                            portableSDDL = null;
                        }
                    }
                }
            }
        }

        private void CreateCloudFileDestinationDirectory(CloudFileDirectory fileDirectory, 
            CloudFileNtfsAttributes? fileAttributes, 
            DateTimeOffset? creationTime,
            DateTimeOffset? lastWriteTime,
            IDictionary<string, string> metadata,
            string portableSDDL,
            CancellationToken cancellationToken)
        {
            bool parentNotExist = false;

            if (fileAttributes.HasValue)
            {
                fileDirectory.Properties.NtfsAttributes = fileAttributes.Value;
                fileDirectory.Properties.CreationTime = creationTime;
                fileDirectory.Properties.LastWriteTime = lastWriteTime;
            }

            if (null != metadata)
            {
                fileDirectory.Metadata.Clear();
                foreach (var keyValuePair in metadata)
                {
                    fileDirectory.Metadata.Add(keyValuePair);
                }
            }

            string filePermissionKey = null;

            if (!string.IsNullOrEmpty(portableSDDL))
            {
                if (portableSDDL.Length > 8 * 1024)
                {
                    filePermissionKey = this.baseDirectoryTransfer.SDDLCache.GetValue(portableSDDL);

                    if (null == filePermissionKey)
                    {
                        filePermissionKey = fileDirectory.Share.CreateFilePermissionAsync(
                            portableSDDL,
                            Transfer_RequestOptions.DefaultFileRequestOptions,
                            Utils.GenerateOperationContext(null),
                            cancellationToken).GetAwaiter().GetResult();

                        this.baseDirectoryTransfer.SDDLCache.AddValue(portableSDDL, filePermissionKey);
                    }

                    fileDirectory.Properties.FilePermissionKey = filePermissionKey;
                }
                else
                {
                    fileDirectory.FilePermission = portableSDDL;
                }
            }

            bool needToSetProperties = false;

            try
            {
                fileDirectory.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null, cancellationToken).GetAwaiter().GetResult();
                return;
            }
            catch (StorageException ex)
            {
                if (null != ex.RequestInformation)
                {
                    if (string.Equals("ParentNotFound", ex.RequestInformation.ErrorCode))
                    {
                        parentNotExist = true;
                    }
                    else if (string.Equals("ResourceAlreadyExists", ex.RequestInformation.ErrorCode))
                    {
                        needToSetProperties = true;
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }

            if (parentNotExist)
            {
                Utils.CreateCloudFileDirectoryRecursively(fileDirectory.Parent);
                try
                {
                    fileDirectory.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null, cancellationToken).GetAwaiter().GetResult();
                }
                catch (StorageException ex)
                {
                    if (null != ex.RequestInformation)
                    {
                        if (string.Equals("ResourceAlreadyExists", ex.RequestInformation.ErrorCode))
                        {
                            needToSetProperties = true;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            if (needToSetProperties)
            {
                SetAzureFileDirectoryAttributes(
                    fileDirectory, 
                    fileAttributes, 
                    creationTime, 
                    lastWriteTime, 
                    filePermissionKey,
                    portableSDDL,
                    metadata, cancellationToken);
            }
        }

        private static void SetAzureFileDirectoryAttributes(
            CloudFileDirectory fileDirectory,
            CloudFileNtfsAttributes? fileAttributes,
            DateTimeOffset? creationTime,
            DateTimeOffset? lastWriteTime,
            string filePermissionKey,
            string portableSDDL,
            IDictionary<string, string> metadata,
            CancellationToken cancellationToken)
        {
            if (fileAttributes.HasValue || null != metadata || !string.IsNullOrEmpty(filePermissionKey) || !string.IsNullOrEmpty(portableSDDL))
            {
                fileDirectory.FetchAttributesAsync(null, Transfer_RequestOptions.DefaultFileRequestOptions, null, cancellationToken).GetAwaiter().GetResult();

                bool needToSetProperties = false;

                if (fileAttributes.HasValue)
                {
                    if (fileDirectory.Properties.NtfsAttributes != fileAttributes.Value
                        || fileDirectory.Properties.CreationTime != creationTime.Value
                        || fileDirectory.Properties.LastWriteTime != lastWriteTime.Value)
                    {
                        needToSetProperties = true;
                        fileDirectory.Properties.NtfsAttributes = fileAttributes.Value;
                        fileDirectory.Properties.CreationTime = creationTime;
                        fileDirectory.Properties.LastWriteTime = lastWriteTime;
                    }
                }

                if (!string.IsNullOrEmpty(filePermissionKey))
                {
                    fileDirectory.Properties.FilePermissionKey = filePermissionKey;
                    needToSetProperties = true;
                }
                else if (!string.IsNullOrEmpty(portableSDDL))
                {
                    fileDirectory.FilePermission = portableSDDL;
                    fileDirectory.Properties.FilePermissionKey = null;
                    needToSetProperties = true;
                }

                if (needToSetProperties)
                {
                    fileDirectory.SetPropertiesAsync(
                        Transfer_RequestOptions.DefaultFileRequestOptions,
                        Utils.GenerateOperationContext(null),
                        cancellationToken).GetAwaiter().GetResult();
                }

                if (null != metadata)
                {
                    if (!metadata.DictionaryEquals(fileDirectory.Metadata))
                    {
                        fileDirectory.Metadata.Clear();
                        foreach (var keyValuePair in metadata)
                        {
                            fileDirectory.Metadata.Add(keyValuePair);
                        }

                        fileDirectory.SetMetadataAsync(null, Transfer_RequestOptions.DefaultFileRequestOptions, null, cancellationToken).GetAwaiter().GetResult();
                    }
                }
            }
        }
    }
}
