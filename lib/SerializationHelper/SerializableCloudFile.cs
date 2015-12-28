//------------------------------------------------------------------------------
// <copyright file="SerializableCloudFile.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// A utility class for serializing and de-serializing <see cref="CloudFile"/> object.
    /// </summary>
    [Serializable]
    internal class SerializableCloudFile : ISerializable
    {
        /// <summary>
        /// Serialization field name for cloud file uri.
        /// </summary>
        private const string FileUriName = "FileUri";

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudFile"/> class.
        /// </summary>
        public SerializableCloudFile()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudFile"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableCloudFile(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            Uri fileUri = (Uri)info.GetValue(FileUriName, typeof(Uri));
            this.CreateCloudFileInstance(fileUri, null);
        }

        /// <summary>
        /// Gets or sets the target <see cref="CloudFile"/> object.
        /// </summary>
        internal CloudFile File
        {
            get;
            set;
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

            Uri fileUri = null;
            if (this.File != null)
            {
                fileUri = this.File.Uri;
            }

            info.AddValue(FileUriName, fileUri, typeof(Uri));
        }

        /// <summary>
        /// Gets the target target <see cref="CloudFile"/> object of a <see cref="SerializableCloudFile"/> object.
        /// </summary>
        /// <param name="fileSerialization">A <see cref="SerializableCloudFile"/> object.</param>
        /// <returns>The target <see cref="CloudFile"/> object.</returns>
        internal static CloudFile GetFile(SerializableCloudFile fileSerialization)
        {
            if (null == fileSerialization)
            {
                return null;
            }

            return fileSerialization.File;
        }

        /// <summary>
        /// Sets the target target <see cref="CloudFile"/> object of a <see cref="SerializableCloudFile"/> object.
        /// </summary>
        /// <param name="fileSerialization">A <see cref="SerializableCloudFile"/> object.</param>
        /// <param name="value">A <see cref="CloudFile"/> object.</param>
        internal static void SetFile(ref SerializableCloudFile fileSerialization, CloudFile value)
        {
            if (null == fileSerialization
                && null == value)
            {
                return;
            }

            if (null != fileSerialization)
            {
                fileSerialization.File = value;
            }
            else
            {
                fileSerialization = new SerializableCloudFile()
                {
                    File = value
                };
            }
        }

        /// <summary>
        /// Updates the account credentials used to access the target <see cref="CloudFile"/> object.
        /// </summary>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            this.CreateCloudFileInstance((this.File == null) ? null : this.File.Uri, credentials);
        }

        /// <summary>
        /// Creates the target <see cref="CloudFile"/> object using the specified uri and account credentials.
        /// </summary>
        /// <param name="fileUri">Cloud file uri.</param>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        private void CreateCloudFileInstance(Uri fileUri, StorageCredentials credentials)
        {
            if (null != this.File
                && this.File.ServiceClient.Credentials == credentials)
            {
                return;
            }

            if (null == fileUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "fileUri"));
            }

            this.File = new CloudFile(fileUri, credentials);
        }
    }
}
