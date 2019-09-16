//------------------------------------------------------------------------------
// <copyright file="UploadDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.Interop;
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

        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB attributes during uploading.
        /// If set to true, destination Azure File's attributes will be set as source local file's attributes.
        /// This flag only takes effect when uploading from local file to Azure File Service.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public bool PreserveSMBAttributes { get; set; }
    }
}
