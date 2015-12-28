//------------------------------------------------------------------------------
// <copyright file="AzureBlobLocation.cs" company="Microsoft">
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

    [Serializable]
    internal class AzureBlobLocation : TransferLocation, ISerializable
    {
        private const string BlobName = "Blob";
        private const string AccessConditionName = "AccessCondition";
        private const string CheckedAccessConditionName = "CheckedAccessCondition";
        private const string RequestOptionsName = "RequestOptions";
        private const string ETagName = "ETag";
        private const string BlockIDPrefixName = "BlockIDPrefix";

        private SerializableCloudBlob blobSerializer;
        private SerializableAccessCondition accessCondition;
        private SerializableRequestOptions requestOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobLocation"/> class.
        /// </summary>
        /// <param name="blob">CloudBlob instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public AzureBlobLocation(CloudBlob blob)
        {
            if (null == blob)
            {
                throw new ArgumentNullException("blob");
            }

            this.Blob = blob;
        }

        private AzureBlobLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.blobSerializer = (SerializableCloudBlob)info.GetValue(BlobName, typeof(SerializableCloudBlob));
            this.accessCondition = (SerializableAccessCondition)info.GetValue(AccessConditionName, typeof(SerializableAccessCondition));
            this.CheckedAccessCondition = info.GetBoolean(CheckedAccessConditionName);
            this.requestOptions = (SerializableBlobRequestOptions)info.GetValue(RequestOptionsName, typeof(SerializableBlobRequestOptions));
            this.ETag = info.GetString(ETagName);
            this.BlockIdPrefix = info.GetString(BlockIDPrefixName);
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureBlob;
            }
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

        /// <summary>
        /// Gets or sets BlobRequestOptions when send request to this location.
        /// </summary>
        internal BlobRequestOptions BlobRequestOptions
        {
            get
            {
                return (BlobRequestOptions)SerializableBlobRequestOptions.GetRequestOptions(this.requestOptions);
            }
            set
            {
                SerializableRequestOptions.SetRequestOptions(ref this.requestOptions, value);
            }
        }

        internal string BlockIdPrefix
        {
            get;
            set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            this.Blob.Container.FetchAttributes(null, Transfer_RequestOptions.DefaultBlobRequestOptions);
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

            info.AddValue(BlobName, this.blobSerializer, typeof(SerializableCloudBlob));

            info.AddValue(AccessConditionName, this.accessCondition, typeof(SerializableAccessCondition));
            info.AddValue(CheckedAccessConditionName, this.CheckedAccessCondition);
            info.AddValue(RequestOptionsName, this.requestOptions, typeof(SerializableBlobRequestOptions));
            info.AddValue(ETagName, this.ETag);
            info.AddValue(BlockIDPrefixName, this.BlockIdPrefix);
        }

        /// <summary>
        /// Update credentials of blob or azure file location.
        /// </summary>
        /// <param name="credentials">Storage credentials to be updated in blob or azure file location.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.blobSerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.Blob.SnapshotQualifiedUri.ToString();
        }
    }
}
