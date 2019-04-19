//------------------------------------------------------------------------------
// <copyright file="TransferEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;

    /// <summary>
    /// Base class of transfer entries.
    /// </summary>
    abstract class TransferEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferEntry" /> class.
        /// </summary>
        /// <param name="relativePath">Relative path of the file indicated by this entry.</param>
        /// <param name="continuationToken">Continuation token when listing to this entry.</param>
        public TransferEntry(string relativePath, ListContinuationToken continuationToken)
        {
            this.RelativePath = relativePath;
            this.ContinuationToken = continuationToken;
        }

        /// <summary>
        /// Gets the relative path of the file indicated by this transfer entry.
        /// </summary>
        public string RelativePath
        {
            get;
            private set;
        }

        /// <summary>
        /// Continuation token when list to this file entry.
        /// </summary>
        public ListContinuationToken ContinuationToken
        {
            get;
            private set;
        }

        abstract public bool IsDirectory
        {
            get;
        }
    }
}
