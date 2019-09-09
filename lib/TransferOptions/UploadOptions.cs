//------------------------------------------------------------------------------
// <copyright file="UploadOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for upload operation
    /// </summary>
    public sealed class UploadOptions
    {
        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the destination object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition DestinationAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets a flag that indicates whether to preserve SMB attributes during uploading.
        /// If set to true, destination Azure File's attributes will be set as source local file's attributes.
        /// This flag only takes effect when uploading from local file to Azure File Service.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
        public bool PreserveSMBAttributes { get; set; }
    }
}
