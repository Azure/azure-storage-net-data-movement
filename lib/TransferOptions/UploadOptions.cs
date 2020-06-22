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
        private PreserveSMBPermissions preserveSMBPermissions = PreserveSMBPermissions.None;

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

        /// <summary>
        /// Gets or sets a value that indicates whether to preserve SMB permissions during uploading.
        /// Preserving SMB permissions is only supported on Windows.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public PreserveSMBPermissions PreserveSMBPermissions
        {
            get
            {
                return this.preserveSMBPermissions;
            }

            set
            {
#if DOTNET5_4
                if ((value != PreserveSMBPermissions.None) && !Interop.CrossPlatformHelpers.IsWindows)
                {
                    throw new PlatformNotSupportedException();
                }
#endif
                this.preserveSMBPermissions = value;
            }
        }

        /// <summary>
        /// Gets or sets a value which specifies the name of the encryption scope to use to encrypt the data provided in the request.
        /// This value only takes effect when destination is Azure Blob Service.
        /// </summary>
        public string EncryptionScope { get; set; }
    }
}
