//------------------------------------------------------------------------------
// <copyright file="SerializableCloudBlob.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// A utility class for serializing and de-serializing <see cref="CloudBlob"/> object.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class SerializableCloudBlob
#if BINARY_SERIALIZATION
        : ISerializable
#endif // BINARY_SERIALIZATION
    {
        /// <summary>
        /// Serialization field name for cloud blob uri.
        /// </summary>
        private const string BlobUriName = "BlobUri";

        /// <summary>
        /// Serialization field name for cloud blob type.
        /// </summary>
        private const string BlobTypeName = "BlobType";

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudBlob"/> class.
        /// </summary>
        public SerializableCloudBlob()
        { 
        }

        #region Serialization helpers

#if !BINARY_SERIALIZATION
        [DataMember] private Uri cloudBlobUri;
        [DataMember] private BlobType cloudBlobType;

        /// <summary>
        /// Serializes the object by extracting key data from the underlying CloudBlob
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            cloudBlobUri = null == this.Blob? null : this.Blob.SnapshotQualifiedUri;
            cloudBlobType = null == this.Blob? BlobType.Unspecified : this.Blob.BlobType;
        }

        /// <summary>
        /// Initializes a deserialized CloudBlob
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            this.CreateCloudBlobInstance(cloudBlobUri, cloudBlobType, null);
        }
#endif // !BINARY_SERIALIZATION
#endregion // Serialization helpers

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudBlob"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableCloudBlob(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            Uri blobUri = (Uri)info.GetValue(BlobUriName, typeof(Uri));
            BlobType blobType = (BlobType)info.GetValue(BlobTypeName, typeof(BlobType));
            this.CreateCloudBlobInstance(blobUri, blobType, null);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets or sets the target <see cref="CloudBlob"/> object.
        /// </summary>
        internal CloudBlob Blob
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
                throw new ArgumentNullException("info");
            }

            Uri blobUri = null;
            BlobType blobType = BlobType.Unspecified;
            if (null != this.Blob)
            {
                blobUri = this.Blob.SnapshotQualifiedUri;
                blobType = this.Blob.BlobType;
            }

            info.AddValue(BlobUriName, blobUri, typeof(Uri));
            info.AddValue(BlobTypeName, blobType, typeof(BlobType));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets the target target <see cref="CloudBlob"/> object of a <see cref="SerializableCloudBlob"/> object.
        /// </summary>
        /// <param name="blobSerialization">A <see cref="SerializableCloudBlob"/> object.</param>
        /// <returns>The target <see cref="CloudBlob"/> object.</returns>
        internal static CloudBlob GetBlob(SerializableCloudBlob blobSerialization)
        {
            if (null == blobSerialization)
            {
                return null;
            }

            return blobSerialization.Blob;
        }

        /// <summary>
        /// Sets the target target <see cref="CloudBlob"/> object of a <see cref="SerializableCloudBlob"/> object.
        /// </summary>
        /// <param name="blobSerialization">A <see cref="SerializableCloudBlob"/> object.</param>
        /// <param name="value">A <see cref="CloudBlob"/> object.</param>
        internal static void SetBlob(ref SerializableCloudBlob blobSerialization, CloudBlob value)
        {
            if ((null == blobSerialization)
                && (null == value))
            {
                return;
            }

            if (null != blobSerialization)
            {
                blobSerialization.Blob = value;
            }
            else
            {
                blobSerialization = new SerializableCloudBlob()
                {
                    Blob = value
                };
            }
        }

        /// <summary>
        /// Updates the account credentials used to access the target <see cref="CloudBlob"/> object.
        /// </summary>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            if (this.Blob == null)
            {
                this.CreateCloudBlobInstance(null, BlobType.Unspecified, credentials);
            }
            else
            {
                this.CreateCloudBlobInstance(this.Blob.SnapshotQualifiedUri, this.Blob.BlobType, credentials);
            }
        }

        /// <summary>
        /// Creates the target <see cref="CloudBlob"/> object using the specified uri, blob type and account crendentials.
        /// </summary>
        /// <param name="blobUri">Cloud blob uri.</param>
        /// <param name="blobType">Cloud blob type.</param>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        private void CreateCloudBlobInstance(Uri blobUri, BlobType blobType, StorageCredentials credentials)
        {
            if ((null != this.Blob)
                && this.Blob.ServiceClient.Credentials == credentials)
            {
                return;
            }

            if (null == blobUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "blobUri"));
            }

            this.Blob = Utils.GetBlobReference(blobUri, credentials, blobType);
        }
    }
}
