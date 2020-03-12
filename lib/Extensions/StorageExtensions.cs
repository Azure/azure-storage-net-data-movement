//------------------------------------------------------------------------------
// <copyright file="BlobExtensions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Extension methods for CloudBlobs for use with BlobTransfer.
    /// </summary>
    internal static class StorageExtensions
    {
        /// <summary>
        /// Determines whether two blobs have the same Uri and SnapshotTime.
        /// </summary>
        /// <param name="blob">Blob to compare.</param>
        /// <param name="comparand">Comparand object.</param>
        /// <returns>True if the two blobs have the same Uri and SnapshotTime; otherwise, false.</returns>
        internal static bool Equals(
            CloudBlob blob,
            CloudBlob comparand)
        {
            if (blob == comparand)
            {
                return true;
            }

            if (null == blob || null == comparand)
            {
                return false;
            }

            return blob.Uri.Equals(comparand.Uri) &&
                blob.SnapshotTime.Equals(comparand.SnapshotTime);
        }

        internal static CloudFile GenerateCopySourceFile(
            this CloudFile file,
            bool preservePermission)
        {
            if (null == file)
            {
                throw new ArgumentNullException("file");
            }

            string sasToken = GetFileSASToken(file, preservePermission);

            if (string.IsNullOrEmpty(sasToken))
            {
                return file;
            }

            return new CloudFile(file.SnapshotQualifiedUri, new StorageCredentials(sasToken));
        }

        internal static Uri GenerateCopySourceUri(this CloudFile file, bool preservePermission = false)
        {
            CloudFile fileForCopy = file.GenerateCopySourceFile(preservePermission);
            return fileForCopy.ServiceClient.Credentials.TransformUri(fileForCopy.SnapshotQualifiedUri);
        }

        private static string GetFileSASToken(CloudFile file, bool preservePermission)
        {
            if (null == file.ServiceClient.Credentials
                || file.ServiceClient.Credentials.IsAnonymous)
            {
                return string.Empty;
            }
            else if (file.ServiceClient.Credentials.IsSAS)
            {
                return file.ServiceClient.Credentials.SASToken;
            }

            // SAS life time is at least 10 minutes.
            TimeSpan sasLifeTime = TimeSpan.FromMinutes(Constants.CopySASLifeTimeInMinutes);

            SharedAccessFilePolicy policy = new SharedAccessFilePolicy()
            {
                SharedAccessExpiryTime = DateTime.Now.Add(sasLifeTime),
                Permissions = SharedAccessFilePermissions.Read,
            };

            return preservePermission ? file.Share.GetSharedAccessSignature(policy) : file.GetSharedAccessSignature(policy);
        }

        /// <summary>
        /// Append an auto generated SAS to a blob uri.
        /// </summary>
        /// <param name="blob">Blob to append SAS.</param>
        /// <returns>Blob Uri with SAS appended.</returns>
        internal static CloudBlob GenerateCopySourceBlob(
            this CloudBlob blob) 
        {
            if (null == blob)
            {
                throw new ArgumentNullException("blob");
            }

            string sasToken = GetBlobSasToken(blob);

            if (string.IsNullOrEmpty(sasToken))
            {
                return blob;
            }

            Uri blobUri = null;

            if (blob.IsSnapshot)
            {
                blobUri = blob.SnapshotQualifiedUri;
            }
            else
            {
                blobUri = blob.Uri;
            }

            return Utils.GetBlobReference(blobUri, new StorageCredentials(sasToken), blob.BlobType);
        }

        internal static Uri GenerateCopySourceUri(this CloudBlob cloudBlob)
        {
            CloudBlob copySourceBlob = cloudBlob.GenerateCopySourceBlob();
            return copySourceBlob.ServiceClient.Credentials.TransformUri(copySourceBlob.SnapshotQualifiedUri);
        }

        internal static string ConvertToString(this object instance)
        {
            CloudBlob blob = instance as CloudBlob;

            if (null != blob)
            {
                return blob.SnapshotQualifiedUri.AbsoluteUri;
            }

            CloudFile file = instance as CloudFile;

            if (null != file)
            {
                return file.SnapshotQualifiedUri.AbsoluteUri;
            }

            CloudFileDirectory cloudFileDirectory = instance as CloudFileDirectory;
            if (null != cloudFileDirectory)
            {
                return cloudFileDirectory.SnapshotQualifiedUri.AbsoluteUri;
            }

            return instance.ToString();
        }

        private static string GetBlobSasToken(CloudBlob blob)
        {
            if (null == blob.ServiceClient.Credentials
                || blob.ServiceClient.Credentials.IsAnonymous)
            {
                return string.Empty;
            }
            else if (blob.ServiceClient.Credentials.IsSAS)
            {
                return blob.ServiceClient.Credentials.SASToken;
            }

            // SAS life time is at least 10 minutes.
            TimeSpan sasLifeTime = TimeSpan.FromMinutes(Constants.CopySASLifeTimeInMinutes);

            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy()
            {
                SharedAccessExpiryTime = DateTime.Now.Add(sasLifeTime),
                Permissions = SharedAccessBlobPermissions.Read,
            };

            CloudBlob rootBlob = null;

            if (!blob.IsSnapshot)
            {
                rootBlob = blob;
            }
            else
            {
                rootBlob = Utils.GetBlobReference(blob.Uri, blob.ServiceClient.Credentials, blob.BlobType);
            }

            return rootBlob.GetSharedAccessSignature(policy);
        }
    }
}
