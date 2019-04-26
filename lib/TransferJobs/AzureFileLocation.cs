//------------------------------------------------------------------------------
// <copyright file="AzureFileLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.DataMovement.SerializationHelper;
    using Microsoft.Azure.Storage.File;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    [KnownType(typeof(SerializableFileRequestOptions))]
    internal class AzureFileLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string AzureFileName = "AzureFile";
        private const string AccessConditionName = "AccessCondition";
        private const string CheckedAccessConditionName = "CheckedAccessCondition";
        private const string RequestOptionsName = "RequestOptions";
        private const string ETagName = "ETag";

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableAccessCondition accessCondition;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableRequestOptions requestOptions;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableCloudFile fileSerializer;

        
        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileLocation"/> class.
        /// </summary>
        /// <param name="azureFile">CloudFile instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public AzureFileLocation(CloudFile azureFile)
        { 
            if (null == azureFile)
            {
                throw new ArgumentNullException("azureFile");
            }

            this.AzureFile = azureFile;
        }

#if BINARY_SERIALIZATION
        private AzureFileLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.fileSerializer = (SerializableCloudFile)info.GetValue(AzureFileName, typeof(SerializableCloudFile));
            this.accessCondition = (SerializableAccessCondition)info.GetValue(AccessConditionName, typeof(SerializableAccessCondition));
            this.CheckedAccessCondition = info.GetBoolean(CheckedAccessConditionName);
            this.requestOptions = (SerializableRequestOptions)info.GetValue(RequestOptionsName, typeof(SerializableRequestOptions));
            this.ETag = info.GetString(ETagName);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureFile;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.AzureFile;
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
        /// Gets the type for this location.
        /// </summary>
        public TransferLocationType TransferLocationType
        {
            get;
            private set;
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

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        internal string ETag
        {
            get;
            set;
        }

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        internal bool CheckedAccessCondition
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets FileRequestOptions when send request to this location.
        /// </summary>
        internal FileRequestOptions FileRequestOptions
        {
            get
            {
                return (FileRequestOptions)SerializableRequestOptions.GetRequestOptions(this.requestOptions);
            }

            set
            {
                SerializableRequestOptions.SetRequestOptions(ref this.requestOptions, value);
            }
        }


        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            try
            {
                this.AzureFile.Parent.FetchAttributesAsync(null, Transfer_RequestOptions.DefaultFileRequestOptions, Utils.GenerateOperationContext(null)).Wait();
            }
            catch (AggregateException e)
            {
                StorageException innnerException = e.Flatten().InnerExceptions[0] as StorageException;
                if (!Utils.IsExpectedHttpStatusCodes(innnerException, HttpStatusCode.Forbidden))
                {
                    throw;
                }
            }
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

            info.AddValue(AzureFileName, this.fileSerializer, typeof(SerializableCloudFile));
            info.AddValue(AccessConditionName, this.accessCondition, typeof(SerializableAccessCondition));
            info.AddValue(CheckedAccessConditionName, this.CheckedAccessCondition);
            info.AddValue(RequestOptionsName, this.requestOptions, typeof(SerializableRequestOptions));
            info.AddValue(ETagName, this.ETag);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Update credentials of blob or azure file location.
        /// </summary>
        /// <param name="credentials">Storage credentials to be updated in blob or azure file location.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.fileSerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.AzureFile.SnapshotQualifiedUri.ToString();
        }
    }
}
