//------------------------------------------------------------------------------
// <copyright file="UploadOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Represents a set of options that may be specified for upload operation
    /// </summary>
    public sealed class UploadOptions
    {
        private bool preserveSMBAttributes = false;

        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the destination object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition DestinationAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB attributes during uploading.
        /// If set to true, destination Azure File's attributes will be set as source local file's attributes.
        /// SMB attributes includes last write time, creation time and <see cref="FileAttributes"/>.
        /// <see cref="FileAttributes"/> will be converted to <see cref="CloudFileNtfsAttributes"/>,
        /// and then set to destination Azure File.
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
