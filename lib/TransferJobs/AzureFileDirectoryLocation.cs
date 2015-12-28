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

    [Serializable]
    internal class AzureFileDirectoryLocation : TransferLocation, ISerializable
    {
        private const string FileDirName = "FileDir";

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

        private AzureFileDirectoryLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.fileDirectorySerializer = (SerializableCloudFileDirectory)info.GetValue(FileDirName, typeof(SerializableCloudFileDirectory));
        }

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

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            this.FileDirectory.CreateIfNotExists(Transfer_RequestOptions.DefaultFileRequestOptions);
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
