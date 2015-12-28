//------------------------------------------------------------------------------
// <copyright file="DownloadDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for download directory operation
    /// </summary>
    public sealed class DownloadDirectoryOptions : DirectoryOptions
    {
        /// <summary>
        /// Gets or sets a flag that indicates whether to validate content MD5 or not when reading data from the source object.
        /// If set to true, source object content MD5 will be validated; otherwise, source object content MD5 will not be validated.
        /// If not specified, it defaults to false.
        /// </summary>
        public bool DisableContentMD5Validation { get; set; }

        /// <summary>
        /// Gets or sets a flag indicating whether to include snapshots when downloading from Azure blob storage.
        /// </summary>
        /// <remarks>
        /// If this flag is set to true, snapshots of the source blob will be copied to destination as
        /// separate files. Given a source blob name in the form of "x.y", where 'x' is the file
        /// name without extension and 'y' is the file name extension, the destination file name of blob
        /// snapshot is formatted as "x (%snapshot_time_stamp%).y".
        /// </remarks>
        public bool IncludeSnapshots { get; set; } 
    }
}
