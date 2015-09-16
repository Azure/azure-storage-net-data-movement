//------------------------------------------------------------------------------
// <copyright file="TransferLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper;
    using Microsoft.WindowsAzure.Storage.File;

    [Serializable]
    internal sealed class TransferLocation : ISerializable
    {
        private const string TransferLocationTypeName = "LocationType";
        private const string FilePathName = "FilePath";
        private const string SourceUriName = "SourceUri";
        private const string BlobName = "Blob";
        private const string AzureFileName = "AzureFile";
        private const string AccessConditionName = "AccessCondition";
        private const string CheckedAccessConditionName = "CheckedAccessCondition";
        private const string RequestOptionsName = "RequestOptions";
        private const string ETagName = "ETag";
        private const string BlockIDPrefixName = "BlockIDPrefix";

        private SerializableAccessCondition accessCondition;
        private SerializableRequestOptions requestOptions;
        private SerializableCloudBlob blobSerializer;
        private SerializableCloudFile fileSerializer;

        private TransferLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.TransferLocationType = (TransferLocationType)info.GetValue(TransferLocationTypeName, typeof(TransferLocationType));

            switch (this.TransferLocationType)
            { 
                case TransferLocationType.FilePath:
                    this.FilePath = info.GetString(FilePathName);
                    break;
                case TransferLocationType.Stream:
                    throw new InvalidOperationException(Resources.CannotSerializeStreamLocation);
                case TransferLocationType.SourceUri:
                    this.SourceUri = (Uri)info.GetValue(SourceUriName, typeof(Uri));
                    break;
                case TransferLocationType.AzureBlob:
                    this.blobSerializer = (SerializableCloudBlob)info.GetValue(BlobName, typeof(SerializableCloudBlob));
                    break;
                case TransferLocationType.AzureFile:
                    this.fileSerializer = (SerializableCloudFile)info.GetValue(AzureFileName, typeof(SerializableCloudFile));
                    break;
                default:
                    break;
            }

            this.accessCondition = (SerializableAccessCondition)info.GetValue(AccessConditionName, typeof(SerializableAccessCondition));
            this.CheckedAccessCondition = info.GetBoolean(CheckedAccessConditionName);
            this.requestOptions = (SerializableRequestOptions)info.GetValue(RequestOptionsName, typeof(SerializableRequestOptions));
            this.ETag = info.GetString(ETagName);
            this.BlockIdPrefix = info.GetString(BlockIDPrefixName);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferLocation"/> class.
        /// </summary>
        /// <param name="filePath">Path to the local file as a source/destination to be read from/written to in a transfer.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1057:StringUriOverloadsCallSystemUriOverloads", Justification="We need to distinct from local file with URI")]
        public TransferLocation(string filePath)
        {
            if (null == filePath)
            {
                throw new ArgumentNullException("filePath");
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("message, should not be an empty string", "filePath");
            }
            this.FilePath = filePath;
            this.TransferLocationType = TransferLocationType.FilePath;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferLocation"/> class.
        /// </summary>
        /// <param name="stream">Stream instance as a source/destination to be read from/written to in a transfer.</param>
        public TransferLocation(Stream stream)
        {
            if (null == stream)
            {
                throw new ArgumentNullException("stream");
            }

            this.Stream = stream;
            this.TransferLocationType = TransferLocationType.Stream;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferLocation"/> class.
        /// </summary>
        /// <param name="blob">Blob instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public TransferLocation(CloudBlob blob)
        {
            if (null == blob)
            {
                throw new ArgumentNullException("blob");
            }

            this.Blob = blob;
            this.TransferLocationType = TransferLocationType.AzureBlob;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferLocation"/> class.
        /// </summary>
        /// <param name="azureFile">CloudFile instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public TransferLocation(CloudFile azureFile)
        { 
            if (null == azureFile)
            {
                throw new ArgumentNullException("azureFile");
            }

            this.AzureFile = azureFile;
            this.TransferLocationType = TransferLocationType.AzureFile;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferLocation"/> class.
        /// </summary>
        /// <param name="sourceUri">Uri to the source in an asynchronously copying job.</param>
        public TransferLocation(Uri sourceUri)
        {
            if (null == sourceUri)
            {
                throw new ArgumentNullException("sourceUri");
            }

            this.SourceUri = sourceUri;
            this.TransferLocationType = TransferLocationType.SourceUri;
        }

        /// <summary>
        /// Gets or sets access condition for this location.
        /// This property only takes effact when the location is a blob or an azure file.
        /// </summary>
        public AccessCondition AccessCondition 
        {
            get
            {
                return SerializableAccessCondition.GetAccessCondition(this.accessCondition);
            }

            set
            {
                SerializableAccessCondition.SetAccessCondition(ref this.accessCondition, value);
            }
        }
        
        /// <summary>
        /// Gets or sets request options when send request to this location.
        /// Only a FileRequestOptions instance takes effact when the location is an azure file;
        /// Only a BlobRequestOptions instance takes effact when the locaiton is a blob.
        /// </summary>
        public IRequestOptions RequestOptions
        {
            get
            {
                return SerializableRequestOptions.GetRequestOptions(this.requestOptions);
            }

            set
            {
                SerializableRequestOptions.SetRequestOptions(ref this.requestOptions, value);
            }
        }

        /// <summary>
        /// Gets the type for this location.
        /// </summary>
        public TransferLocationType TransferLocationType
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets path to the local file location.
        /// </summary>
        public string FilePath
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets an stream instance representing the location for this instance.
        /// </summary>
        public Stream Stream
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets Uri to the source location in asynchronously copying job.
        /// </summary>
        public Uri SourceUri
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets blob location in this instance.
        /// </summary>
        public CloudBlob Blob
        {
            get
            {
                return SerializableCloudBlob.GetBlob(this.blobSerializer);
            }

            private set
            {
                SerializableCloudBlob.SetBlob(ref this.blobSerializer, value);
            }
        }

        /// <summary>
        /// Gets azure file location in this instance.
        /// </summary>
        public CloudFile AzureFile
        {
            get
            {
                return SerializableCloudFile.GetFile(this.fileSerializer);
            }

            private set
            {
                SerializableCloudFile.SetFile(ref this.fileSerializer, value);
            }
        }

        internal string ETag
        {
            get;
            set;
        }

        internal bool CheckedAccessCondition
        {
            get;
            set;
        }

        internal BlobRequestOptions BlobRequestOptions
        {
            get
            {
                return this.RequestOptions as BlobRequestOptions;
            }
        }

        internal FileRequestOptions FileRequestOptions
        {
            get
            {
                return this.RequestOptions as FileRequestOptions;
            }
        }

        internal string BlockIdPrefix
        {
            get;
            set;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1057:StringUriOverloadsCallSystemUriOverloads", Justification="We need to distinct from local file with URI")]
        public static implicit operator TransferLocation(string filePath)
        {
            return new TransferLocation(filePath);
        }

        public static implicit operator TransferLocation(Stream stream)
        {
            return new TransferLocation(stream);
        }

        public static implicit operator TransferLocation(CloudBlockBlob blob)
        {
            return new TransferLocation(blob);
        }

        public static implicit operator TransferLocation(CloudPageBlob blob)
        {
            return new TransferLocation(blob);
        }

        public static implicit operator TransferLocation(CloudFile azureFile)
        {
            return new TransferLocation(azureFile);
        }

        public static implicit operator TransferLocation(Uri sourceUri)
        {
            return ToTransferLocation(sourceUri);
        }

        public static TransferLocation ToTransferLocation(Uri sourceUri)
        {
            return new TransferLocation(sourceUri);
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
                throw new System.ArgumentNullException("info");
            }

            info.AddValue(TransferLocationTypeName, this.TransferLocationType);

            switch (this.TransferLocationType)
            { 
                case TransferLocationType.FilePath:
                    info.AddValue(FilePathName, this.FilePath);
                    break;
                case TransferLocationType.SourceUri:
                    info.AddValue(SourceUriName, this.SourceUri, typeof(Uri));
                    break;
                case TransferLocationType.AzureBlob:
                    info.AddValue(BlobName, this.blobSerializer, typeof(SerializableCloudBlob));
                    break;
                case TransferLocationType.AzureFile:
                    info.AddValue(AzureFileName, this.fileSerializer, typeof(SerializableCloudFile));
                    break;
                case TransferLocationType.Stream:
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.CannotDeserializeLocationType,
                        this.TransferLocationType));
            }

            info.AddValue(AccessConditionName, this.accessCondition, typeof(SerializableAccessCondition));
            info.AddValue(CheckedAccessConditionName, this.CheckedAccessCondition);
            info.AddValue(RequestOptionsName, this.requestOptions, typeof(SerializableRequestOptions));
            info.AddValue(ETagName, this.ETag);
            info.AddValue(BlockIDPrefixName, this.BlockIdPrefix);
        }

        /// <summary>
        /// Update credentials of blob or azure file location.
        /// </summary>
        /// <param name="credentials">Storage credentials to be updated in blob or azure file location.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            if (null != this.blobSerializer)
            {
                this.blobSerializer.UpdateStorageCredentials(credentials);
            }
            else if (null != this.fileSerializer)
            {
                this.fileSerializer.UpdateStorageCredentials(credentials);
            }
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            switch(this.TransferLocationType)
            {
                case TransferLocationType.FilePath:
                    return this.FilePath;

                case TransferLocationType.AzureBlob:
                    return this.Blob.SnapshotQualifiedUri.ToString();

                case TransferLocationType.AzureFile:
                    return this.AzureFile.Uri.ToString();

                case TransferLocationType.SourceUri:
                    return this.SourceUri.ToString();

                case TransferLocationType.Stream:
                    return this.Stream.ToString();

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        // Summary:
        //     Determines whether the specified transfer location is equal to the current transfer location.
        //
        // Parameters:
        //   obj:
        //     The transfer location to compare with the current transfer location.
        //
        // Returns:
        //     true if the specified transfer location is equal to the current transfer location; otherwise, false.
        public override bool Equals(object obj)
        {
            TransferLocation location = obj as TransferLocation;
            if (location == null || this.TransferLocationType != location.TransferLocationType)
                return false;

            switch (this.TransferLocationType)
            {
                case TransferLocationType.AzureBlob:
                case TransferLocationType.AzureFile:
                case TransferLocationType.FilePath:
                case TransferLocationType.SourceUri:
                    return this.ToString() == location.ToString();

                case TransferLocationType.Stream:
                default:
                    return false;
            }
        }

        //
        // Summary:
        //     Returns the hash code for the transfer location.
        //
        // Returns:
        //     A 32-bit signed integer hash code.
        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }
    }
}
