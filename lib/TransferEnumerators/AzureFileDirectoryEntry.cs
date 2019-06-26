//------------------------------------------------------------------------------
// <copyright file="AzureFileDirectoryEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// AzureFileEntry class to represent an azure file directory entry on Azure file service.
    /// </summary>
    internal class AzureFileDirectoryEntry : TransferEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AzureFileDirectoryEntry" /> class.
        /// </summary>
        /// <param name="relativePath">Relative path of the directory indicated by this directory entry.</param>
        /// <param name="cloudFileDirectory">Corresponding CloudFileDirectory instance.</param>
        /// <param name="continuationToken">Continuation token when listing to this entry.</param>
        public AzureFileDirectoryEntry(string relativePath, CloudFileDirectory cloudFileDirectory, AzureFileListContinuationToken continuationToken)
            : base(relativePath, continuationToken)
        {
            this.FileDirectory = cloudFileDirectory;
        }

        /// <summary>
        /// Gets the reference to the cloud file directory.
        /// </summary>
        public CloudFileDirectory FileDirectory
        {
            get;
            private set;
        }

        public override bool IsDirectory { get => true; }

        public override string ToString()
        {
            return FileDirectory.ConvertToString();
        }
    }
}