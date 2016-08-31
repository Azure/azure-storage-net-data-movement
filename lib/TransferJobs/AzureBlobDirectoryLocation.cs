//------------------------------------------------------------------------------
// <copyright file="AzureBlobDirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class AzureBlobDirectoryLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string BlobDirName = "CloudBlobDir";

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableCloudBlobDirectory blobDirectorySerializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobDirectoryLocation"/> class.
        /// </summary>
        /// <param name="blobDir">CloudBlobDirectory instance as a location in a transfer job.
        /// It could be a source, a destination.</param>
        public AzureBlobDirectoryLocation(CloudBlobDirectory blobDir)
        {
            if (null == blobDir)
            {
                throw new ArgumentNullException("blobDir");
            }

            this.blobDirectorySerializer = new SerializableCloudBlobDirectory(blobDir);
        }

#if BINARY_SERIALIZATION
        private AzureBlobDirectoryLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.blobDirectorySerializer = (SerializableCloudBlobDirectory)info.GetValue(BlobDirName, typeof(SerializableCloudBlobDirectory));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureBlobDirectory;
            }
        }

        /// <summary>
        /// Gets Azure blob directory location in this instance.
        /// </summary>
        public CloudBlobDirectory BlobDirectory
        {
            get
            {
                return this.blobDirectorySerializer.BlobDirectory;
            }
        }

        /// <summary>
        /// Gets or sets BlobRequestOptions when send request to this location.
        /// </summary>
        internal BlobRequestOptions BlobRequestOptions
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

            info.AddValue(BlobDirName, this.blobDirectorySerializer, typeof(SerializableCloudBlobDirectory));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            this.BlobDirectory.Container.FetchAttributesAsync(null, Transfer_RequestOptions.DefaultBlobRequestOptions, null).Wait();
        }

        /// <summary>
        /// Update credentials of azure blob directory location.
        /// </summary>
        /// <param name="credentials">New storage credentials to use.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.blobDirectorySerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.BlobDirectory.Uri.ToString();
        }
    }
}
