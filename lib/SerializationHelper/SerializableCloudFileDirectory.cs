//------------------------------------------------------------------------------
// <copyright file="SerializableCloudFileDirectory.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// A utility class for serializing and de-serializing <see cref="CloudFileDirectory"/> object.
    /// </summary>
    [Serializable]
    internal class SerializableCloudFileDirectory : ISerializable
    {
        /// <summary>
        /// Serialization field name for cloud file directory uri.
        /// </summary>
        private const string FileDirectoryUriName = "FileDirectoryUri";

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudFileDirectory" /> class.
        /// </summary>
        /// <param name="fileDir">A <see cref="CloudFileDirectory" /> object. </param>
        public SerializableCloudFileDirectory(CloudFileDirectory fileDir)
        {
            this.FileDirectory = fileDir;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudFileDirectory"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableCloudFileDirectory(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            Uri fileDirectoryUri = (Uri)info.GetValue(FileDirectoryUriName, typeof(Uri));
            this.CreateCloudFileDirectoryInstance(fileDirectoryUri, null);
        }

        /// <summary>
        /// Gets the target <see cref="CloudFileDirectory" /> object.
        /// </summary>
        internal CloudFileDirectory FileDirectory
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
                throw new ArgumentNullException("info");
            }

            Uri fileDirectoryUri = null;
            if (this.FileDirectory != null)
            {
                fileDirectoryUri = this.FileDirectory.Uri;
            }

            info.AddValue(FileDirectoryUriName, fileDirectoryUri, typeof(Uri));
        }

        /// <summary>
        /// Updates the account credentials used to access the target <see cref="CloudFileDirectory"/> object.
        /// </summary>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            if (null != this.FileDirectory && this.FileDirectory.ServiceClient.Credentials == credentials)
            {
                return;
            }

            this.CreateCloudFileDirectoryInstance(this.FileDirectory == null ? null : this.FileDirectory.Uri, credentials);
        }

        /// <summary>
        /// Creates the target <see cref="CloudFileDirectory"/> object using the specified uri and account credentials.
        /// </summary>
        /// <param name="fileDirectoryUri">Cloud file directory uri.</param>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        private void CreateCloudFileDirectoryInstance(Uri fileDirectoryUri, StorageCredentials credentials)
        {
            if (null == fileDirectoryUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "fileDirectoryUri"));
            }

            this.FileDirectory = new CloudFileDirectory(fileDirectoryUri, credentials);
        }
    }
}
