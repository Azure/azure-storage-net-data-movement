//------------------------------------------------------------------------------
// <copyright file="AzureFileDirectoryLocation.cs" company="Microsoft">
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
    internal class AzureFileDirectoryLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string FileDirName = "FileDir";
        private const string RequestOptionsName = "RequestOptions";

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableCloudFileDirectory fileDirectorySerializer;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private SerializableRequestOptions requestOptions;

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
            this.requestOptions = (SerializableRequestOptions)info.GetValue(RequestOptionsName, typeof(SerializableRequestOptions));
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
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.FileDirectory;
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
            get
            {
                return (FileRequestOptions)SerializableRequestOptions.GetRequestOptions(this.requestOptions);
            }

            set
            {
                SerializableRequestOptions.SetRequestOptions(ref this.requestOptions, value);
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

            info.AddValue(FileDirName, this.fileDirectorySerializer, typeof(SerializableCloudFileDirectory));
            info.AddValue(RequestOptionsName, this.requestOptions, typeof(SerializableRequestOptions));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            try
            {
                this.FileDirectory.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, Utils.GenerateOperationContext(null)).Wait();
            }
            catch(AggregateException e)
            {
                StorageException innnerException = e.Flatten().InnerExceptions[0] as StorageException;
                if (!Utils.IsExpectedHttpStatusCodes(
                        innnerException, 
                        this.FileDirectory.Parent == null ? HttpStatusCode.MethodNotAllowed : HttpStatusCode.Conflict, // Create root directory of a share causes 405 error.
                        HttpStatusCode.Forbidden,
                        HttpStatusCode.NotFound))
                {
                    throw;
                }
            }
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
            return this.FileDirectory.SnapshotQualifiedUri.ToString();
        }
    }
}
