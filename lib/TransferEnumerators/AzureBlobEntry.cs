//------------------------------------------------------------------------------
// <copyright file="AzureBlobEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// AzureBlobEntry class to represent a single transfer entry on Azure blob service.
    /// </summary>
    internal class AzureBlobEntry : TransferEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobEntry" /> class.
        /// </summary>
        /// <param name="relativePath">Relative path of the blob indicated by this blob entry.</param>
        /// <param name="cloudBlob">Corresponding CloudBlob.</param>
        /// <param name="continuationToken">Continuation token when listing to this entry.</param>
        public AzureBlobEntry(string relativePath, CloudBlob cloudBlob, AzureBlobListContinuationToken continuationToken)
            : base(relativePath, continuationToken)
        {
            this.Blob = cloudBlob;
        }

        /// <summary>
        /// Gets the reference to the blob.
        /// </summary>
        public CloudBlob Blob
        {
            get;
            private set;
        }

        public override string ToString()
        {
            return Blob.ConvertToString();
        }

        public override bool IsDirectory { get => false; }
    }
}