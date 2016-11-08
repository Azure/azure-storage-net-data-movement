//------------------------------------------------------------------------------
// <copyright file="UploadDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Represents a set of options that may be specified for upload directory operation
    /// </summary>
    public sealed class UploadDirectoryOptions : DirectoryOptions
    {
        /// <summary>
        /// Gets or sets type of destination blob. This option takes effect only when uploading to Azure blob storage.
        /// If blob type is not specified, BlockBlob is used.
        /// </summary>
        public BlobType BlobType { get; set; }
    }
}
