//------------------------------------------------------------------------------
// <copyright file="SerializableCloudBlob.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    [Serializable]
    internal class SerializableCloudBlob : ISerializable
    {
        private const string BlobUriName = "BlobUri";
        private const string BlobTypeName = "BlobType";

        private Uri blobUri;

        private BlobType blobType;

        private CloudBlob blob;

        public SerializableCloudBlob()
        { 
        }

        private SerializableCloudBlob(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            this.blobUri = (Uri)info.GetValue(BlobUriName, typeof(Uri));
            this.blobType = (BlobType)info.GetValue(BlobTypeName, typeof(BlobType));
            this.CreateCloudBlobInstance(null);
        }

        internal CloudBlob Blob
        {
            get
            {
                return this.blob;
            }

            set
            {
                this.blob = value;

                if (null == this.blob)
                {
                    this.blobUri = null;
                    this.blobType = BlobType.Unspecified;
                }
                else
                {
                    this.blobUri = this.blob.SnapshotQualifiedUri;
                    this.blobType = this.blob.BlobType;
                }
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(BlobUriName, this.blobUri, typeof(Uri));
            info.AddValue(BlobTypeName, this.blobType);
        }

        internal static CloudBlob GetBlob(SerializableCloudBlob blobSerialization)
        {
            if (null == blobSerialization)
            {
                return null;
            }

            return blobSerialization.Blob;
        }

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

        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            this.CreateCloudBlobInstance(credentials);
        }

        private void CreateCloudBlobInstance(StorageCredentials credentials)
        {
            if ((null != this.blob)
                && this.blob.ServiceClient.Credentials == credentials)
            {
                return;
            }

            if (null == this.blobUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "blobUri"));
            }

            this.blob = Utils.GetBlobReference(this.blobUri, credentials, this.blobType);
        }
    }
}
