//------------------------------------------------------------------------------
// <copyright file="SerializableBlobRequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Blob;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal sealed class SerializableBlobRequestOptions : SerializableRequestOptions
    {
        private const string DisableContentMD5ValidationName = "DisableContentMD5Validation";
        private const string MaximumExecutionTimeName = "MaximumExecutionTime";
        private const string ServerTimeoutName = "ServerTimeout";
        private const string StoreBlobContentMD5Name = "StoreBlobContentMD5";
        private const string UseTransactionalMD5Name = "UseTransactionalMD5";

        private BlobRequestOptions blobRequestOptions;

        public SerializableBlobRequestOptions()
        {
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableBlobRequestOptions"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableBlobRequestOptions(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            bool? disableContentMD5Validation = (bool?)info.GetValue(DisableContentMD5ValidationName, typeof(bool?));
            TimeSpan? maximumExecutionTime = (TimeSpan?)info.GetValue(MaximumExecutionTimeName, typeof(TimeSpan?));
            TimeSpan? serverTimeout = (TimeSpan?)info.GetValue(ServerTimeoutName, typeof(TimeSpan?));
            bool? storeBlobContentMD5 = (bool?)info.GetValue(StoreBlobContentMD5Name, typeof(bool?));
            bool? useTransactionalMD5 = (bool?)info.GetValue(UseTransactionalMD5Name, typeof(bool?));

            if (null != disableContentMD5Validation
                || null != maximumExecutionTime
                || null != serverTimeout
                || null != storeBlobContentMD5
                || null != useTransactionalMD5)
            {
                this.blobRequestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;

                this.blobRequestOptions.DisableContentMD5Validation = disableContentMD5Validation;
                this.blobRequestOptions.MaximumExecutionTime = maximumExecutionTime;
                this.blobRequestOptions.ServerTimeout = serverTimeout;
                this.blobRequestOptions.StoreBlobContentMD5 = storeBlobContentMD5;
                this.blobRequestOptions.UseTransactionalMD5 = useTransactionalMD5;
            }
            else
            {
                this.blobRequestOptions = null;
            }
        }
#endif // BINARY_SERIALIZATION

#region Serialization helpers

#if !BINARY_SERIALIZATION
        [DataMember]
        private bool? disableContentMD5Validation;
        [DataMember]
        private TimeSpan? maximumExecutionTime;
        [DataMember]
        private TimeSpan? serverTimeout;
        [DataMember]
        private bool? storeBlobContentMD5;
        [DataMember]
        private bool? useTransactionalMD5;

        /// <summary>
        /// Serializes the object by extracting key data from the underlying BlobRequestOptions
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            disableContentMD5Validation = null == blobRequestOptions ? null : this.blobRequestOptions.DisableContentMD5Validation;
            maximumExecutionTime = null == blobRequestOptions ? null : this.blobRequestOptions.MaximumExecutionTime;
            serverTimeout = null == blobRequestOptions ? null : this.blobRequestOptions.ServerTimeout;
            storeBlobContentMD5 = null == blobRequestOptions ? null : this.blobRequestOptions.StoreBlobContentMD5;
            useTransactionalMD5 = null == blobRequestOptions ? null : this.blobRequestOptions.UseTransactionalMD5;
        }

        /// <summary>
        /// Initializes a deserialized BlobRequestOptions
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            if (null != disableContentMD5Validation
                || null != maximumExecutionTime
                || null != serverTimeout
                || null != storeBlobContentMD5
                || null != useTransactionalMD5)
            {
                this.blobRequestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;

                this.blobRequestOptions.DisableContentMD5Validation = disableContentMD5Validation;
                this.blobRequestOptions.MaximumExecutionTime = maximumExecutionTime;
                this.blobRequestOptions.ServerTimeout = serverTimeout;
                this.blobRequestOptions.StoreBlobContentMD5 = storeBlobContentMD5;
                this.blobRequestOptions.UseTransactionalMD5 = useTransactionalMD5;
            }
            else
            {
                this.blobRequestOptions = null;
            }
        }
#endif // !BINARY_SERIALIZATION
#endregion // Serialization helpers

        protected override IRequestOptions RequestOptions
        {
            get
            {
                return this.blobRequestOptions;
            }

            set
            {
                BlobRequestOptions requestOptions = value as BlobRequestOptions;
                Debug.Assert(null != requestOptions, "Setting RequestOptions in BlobRequestOptionsSerializer, but the value is not a BlobRequestOptions instance.");
                this.blobRequestOptions = requestOptions;
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

            if (null == this.blobRequestOptions)
            {
                info.AddValue(DisableContentMD5ValidationName, null);
                info.AddValue(MaximumExecutionTimeName, null, typeof(TimeSpan?));
                info.AddValue(ServerTimeoutName, null, typeof(TimeSpan?));
                info.AddValue(StoreBlobContentMD5Name, null);
                info.AddValue(UseTransactionalMD5Name, null);
            }
            else
            {
                info.AddValue(DisableContentMD5ValidationName, this.blobRequestOptions.DisableContentMD5Validation);
                info.AddValue(MaximumExecutionTimeName, this.blobRequestOptions.MaximumExecutionTime, typeof(TimeSpan?));
                info.AddValue(ServerTimeoutName, this.blobRequestOptions.ServerTimeout, typeof(TimeSpan?));
                info.AddValue(StoreBlobContentMD5Name, this.blobRequestOptions.StoreBlobContentMD5);
                info.AddValue(UseTransactionalMD5Name, this.blobRequestOptions.UseTransactionalMD5);
            }
        }
#endif // BINARY_SERIALIZATION
    }
}
