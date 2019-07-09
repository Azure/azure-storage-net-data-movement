//------------------------------------------------------------------------------
// <copyright file="AzureFileEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// AzureFileEntry class to represent a single transfer entry on Azure file service.
    /// </summary>
    internal class AzureFileEntry : TransferEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileEntry" /> class.
        /// </summary>
        /// <param name="relativePath">Relative path of the file indicated by this file entry.</param>
        /// <param name="cloudFile">Corresponding CloudFile.</param>
        /// <param name="continuationToken">Continuation token when listing to this entry.</param>
        public AzureFileEntry(string relativePath, CloudFile cloudFile, AzureFileListContinuationToken continuationToken)
            : base(relativePath, continuationToken)
        {
            this.File = cloudFile;
        }

        /// <summary>
        /// Gets the reference to the cloud file.
        /// </summary>
        public CloudFile File
        {
            get;
            private set;
        }

        public override bool IsDirectory { get => false; }

        public override string ToString()
        {
            return File.ConvertToString();
        }
    }
}