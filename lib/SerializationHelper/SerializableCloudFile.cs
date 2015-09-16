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

    [Serializable]
    internal class SerializableCloudFile : ISerializable
    {
        private const string FileUriName = "FileUri";

        private Uri fileUri;

        private CloudFile file;

        public SerializableCloudFile()
        {
        }

        private SerializableCloudFile(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            this.fileUri = (Uri)info.GetValue(FileUriName, typeof(Uri));
            this.CreateCloudFileInstance(null);
        }

        internal CloudFile File
        {
            get
            {
                return this.file;
            }

            set
            {
                this.file = value;

                if (null == this.file)
                {
                    this.fileUri = null;
                }
                else
                {
                    this.fileUri = this.file.Uri;
                }
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(FileUriName, this.fileUri, typeof(Uri));
        }

        internal static CloudFile GetFile(SerializableCloudFile fileSerialization)
        {
            if (null == fileSerialization)
            {
                return null;
            }

            return fileSerialization.File;
        }

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

        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            this.CreateCloudFileInstance(credentials);
        }

        private void CreateCloudFileInstance(StorageCredentials credentials)
        {
            if (null != this.file
                && this.file.ServiceClient.Credentials == credentials)
            {
                return;
            }

            if (null == this.fileUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "fileUri"));
            }

            this.file = new CloudFile(this.fileUri, credentials);
        }
    }
}
