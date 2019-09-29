//------------------------------------------------------------------------------
// <copyright file="DownloadOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Represents a set of options that may be specified for download operation
    /// </summary>
    public sealed class DownloadOptions
    {
        private bool preserveSMBAttributes = false;

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

        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB attributes during downloading.
        /// If set to true, destination Azure File's attributes will be set as source local file's attributes.
        /// SMB attributes includes last write time, creation time and <see cref="CloudFileNtfsAttributes"/>.
        /// <see cref="CloudFileNtfsAttributes"/> will be converted to <see cref="FileAttributes"/> ,
        /// and then set to destination local file.
        /// This flag only takes effect when uploading from local file to Azure File Service.
        /// Preserving SMB attributes is only supported on Windows.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public bool PreserveSMBAttributes
        {
            get
            {
                return this.preserveSMBAttributes;
            }

            set
            {
#if DOTNET5_4
                if (value && !Interop.CrossPlatformHelpers.IsWindows)
                {
                    throw new PlatformNotSupportedException();
                }
                else
                {
                    this.preserveSMBAttributes = value;
                }
#else
                this.preserveSMBAttributes = value;
#endif
            }
        }
    }
}
