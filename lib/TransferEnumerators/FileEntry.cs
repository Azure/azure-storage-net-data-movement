﻿//------------------------------------------------------------------------------
// <copyright file="FileEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;

    /// <summary>
    /// FileEntry class to represent a single transfer entry on file system.
    /// </summary>
    internal class FileEntry : TransferEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FileEntry" /> class.
        /// </summary>
        /// <param name="relativePath">Relative path of the file indicated by this file entry.</param>
        /// <param name="fullPath">Full path of the file indicated by this file entry.</param>
        /// <param name="continuationToken">Continuation token when listing to this entry.</param>
        public FileEntry(string relativePath, string fullPath, FileListContinuationToken continuationToken)
            : base(relativePath, continuationToken)
        {
            this.FullPath = fullPath;
        }

        /// <summary>
        /// Gets the full path of the file.
        /// </summary>
        public string FullPath
        {
            get;
            private set;
        }

        public override string ToString()
        {
            return FullPath;
        }
    }
}
