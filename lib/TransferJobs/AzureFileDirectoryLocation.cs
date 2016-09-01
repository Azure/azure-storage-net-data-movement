//------------------------------------------------------------------------------
// <copyright file="AzureFileDirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper;
    using Microsoft.WindowsAzure.Storage.File;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class AzureFileDirectoryLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string FileDirName = "FileDir";
        
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableCloudFileDirectory fileDirectorySerializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileDirectoryLocation"/> class.
        /// </summary>
        /// <param name="fileDir">CloudFileDirectory instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public AzureFileDirectoryLocation(CloudFileDirectory fileDir)
        { 
            if (null == fileDir)
            {
                throw new ArgumentNullException("fileDir");
            }

            this.fileDirectorySerializer = new SerializableCloudFileDirectory(fileDir);
        }

#if BINARY_SERIALIZATION
        private AzureFileDirectoryLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.fileDirectorySerializer = (SerializableCloudFileDirectory)info.GetValue(FileDirName, typeof(SerializableCloudFileDirectory));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureFileDirectory;
            }
        }

        /// <summary>
        /// Gets Azure file directory location in this instance.
        /// </summary>
        public CloudFileDirectory FileDirectory
        {
            get
            {
                return this.fileDirectorySerializer.FileDirectory;
            }
        }

        /// <summary>
        /// Gets or sets FileRequestOptions when send request to this location.
        /// </summary>
        internal FileRequestOptions FileRequestOptions
        {
            get;
            set;
        }

#if BINARY_SERIALIZATION
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

            info.AddValue(FileDirName, this.fileDirectorySerializer, typeof(SerializableCloudFileDirectory));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            this.FileDirectory.CreateIfNotExistsAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).Wait();
        }

        /// <summary>
        /// Update credentials of Azure file directory location.
        /// </summary>
        /// <param name="credentials">New storage credentials to use.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.fileDirectorySerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.FileDirectory.Uri.ToString();
        }
    }
}
