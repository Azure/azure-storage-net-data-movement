//------------------------------------------------------------------------------
// <copyright file="AzureBlobListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Blob;

    [Serializable]
    internal sealed class AzureBlobListContinuationToken : ListContinuationToken, ISerializable
    {
        private const string BlobContinuationTokenName = "BlobContinuationToken";
        private const string BlobNameName = "BlobName";
        private const string HasSnapshotName = "HasSnapshot";
        private const string SnapshotTimeName = "SnapshotTime";

        public AzureBlobListContinuationToken(BlobContinuationToken blobContinuationToken, string blobName, DateTimeOffset? snapshotTime)
        {
            this.BlobContinuationToken = blobContinuationToken;
            this.BlobName = blobName;
            this.SnapshotTime = snapshotTime;
        }

        private AzureBlobListContinuationToken(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.BlobContinuationToken = (BlobContinuationToken)info.GetValue(BlobContinuationTokenName, typeof(BlobContinuationToken));
            this.BlobName = info.GetString(BlobNameName);

            if (info.GetBoolean(HasSnapshotName))
            {
                this.SnapshotTime = (DateTimeOffset)info.GetValue(SnapshotTimeName, typeof(DateTimeOffset));
            }
            else
            {
                this.SnapshotTime = null;
            }
        }

        public BlobContinuationToken BlobContinuationToken
        {
            get;
            private set;
        }

        public string BlobName
        {
            get;
            private set;
        }

        public DateTimeOffset? SnapshotTime
        {
            get;
            private set;
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

            info.AddValue(BlobContinuationTokenName, this.BlobContinuationToken, typeof(BlobContinuationToken));
            info.AddValue(BlobNameName, this.BlobName, typeof(string));
            info.AddValue(HasSnapshotName, this.SnapshotTime.HasValue);

            if (this.SnapshotTime.HasValue)
            { 
                info.AddValue(SnapshotTimeName, this.SnapshotTime.Value, typeof(DateTimeOffset));
            }
        }
    }
}
