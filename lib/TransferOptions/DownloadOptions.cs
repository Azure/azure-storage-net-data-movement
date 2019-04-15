//------------------------------------------------------------------------------
// <copyright file="DownloadOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for download operation
    /// </summary>
    public sealed class DownloadOptions
    {
        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the source object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition SourceAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets a flag that indicates whether to validate content MD5 or not when reading data from the source object.
        /// If set to true, source object content MD5 will be validated; otherwise, source object content MD5 will not be validated.
        /// If not specified, it defaults to false.
        /// </summary>
        public bool DisableContentMD5Validation { get; set; }
    }
}
