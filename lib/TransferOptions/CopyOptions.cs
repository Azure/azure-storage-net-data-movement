//------------------------------------------------------------------------------
// <copyright file="CopyOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using Microsoft.Azure.Storage.File;
    /// <summary>
    /// Represents a set of options that may be specified for copy operation
    /// </summary>
    public sealed class CopyOptions
    {
        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB attributes during copying.
        /// If set to true, destination Azure File's attributes will be set as source local file's attributes.
        /// SMB attributes includes last write time, creation time and <see cref="CloudFileNtfsAttributes"/>.
        /// This flag only takes effect when copying from Azure File Service to Azure File Service.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public bool PreserveSMBAttributes { get; set; }

        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB permissions during copying.
        /// This flag only takes effect when copying from Azure File Service to Azure File Service.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public bool PreserveSMBPermissions { get; set; }

        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the source object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition SourceAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the destination object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition DestinationAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets a value which specifies the name of the encryption scope to use to encrypt the data provided in the request.
        /// This value only takes effect when destination is Azure Blob Service.
        /// </summary>
        public string EncryptionScope { get; set; }
    }
}
