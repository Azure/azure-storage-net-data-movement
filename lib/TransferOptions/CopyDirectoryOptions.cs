//------------------------------------------------------------------------------
// <copyright file="CopyDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Represents a set of options that may be specified for copy directory operation
    /// </summary>
    public sealed class CopyDirectoryOptions : DirectoryOptions
    {
        private char delimiter = '/';

        /// <summary>
        /// Gets or sets type of destination blob. This option takes effect only when copying from non Azure
        /// blob storage to Azure blob storage. If blob type is not specified, BlockBlob is used.
        /// </summary>
        public BlobType BlobType { get; set; }

        /// <summary>
        /// Gets or sets a flag indicating whether to include snapshots when copying from Azure blob storage.
        /// </summary>
        /// <remarks>
        /// If this flag is set to true, snapshots of the source blob will be copied to destination as
        /// separate files. Given a source blob name in the form of "x.y", where 'x' is the file
        /// name without extension and 'y' is the file name extension, the destination file name of blob
        /// snapshot is formatted as "x (%snapshot_time_stamp%).y".
        /// </remarks>
        public bool IncludeSnapshots { get; set; }

        /// <summary>
        /// Gets or sets a char that indicates the delimiter character used to delimit virtual directories in a blob name.
        /// </summary>
        public char Delimiter
        {
            get
            {
                return this.delimiter;
            }
            set
            {
                this.delimiter = value;
            }
        }

        /// <summary>
        /// Gets or sets a value which specifies the name of the encryption scope to use to encrypt the data provided in the request.
        /// This value only takes effect when destination is Azure Blob Service.
        /// </summary>
        public string EncryptionScope { get; set; }
    }
}
