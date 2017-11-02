//------------------------------------------------------------------------------
// <copyright file="UploadDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;
    using System;

    /// <summary>
    /// Represents a set of options that may be specified for upload directory operation
    /// </summary>
    public sealed class UploadDirectoryOptions : DirectoryOptions
    {
        private bool followSymlink = false;
        /// <summary>
        /// Gets or sets type of destination blob. This option takes effect only when uploading to Azure blob storage.
        /// If blob type is not specified, BlockBlob is used.
        /// </summary>
        public BlobType BlobType { get; set; }

        /// <summary>
        /// Gets or sets whether to follow symlinked directories. This option only works in Unix/Linux platforms.
        /// </summary>
        public bool FollowSymlink
        {
            get
            {
                return this.followSymlink;
            }

            set
            {
                if (value && !CrossPlatformHelpers.IsLinux)
                {
                    throw new PlatformNotSupportedException();
                }

                this.followSymlink = value;
            }
        }
    }
}
