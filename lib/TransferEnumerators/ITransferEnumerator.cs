//------------------------------------------------------------------------------
// <copyright file="ITransferEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// Transfer enumerator interface.
    /// </summary>
    internal interface ITransferEnumerator
    {
        /// <summary>
        /// Gets or sets the file enumerate continulation token.
        /// </summary>
        ListContinuationToken EnumerateContinuationToken
        {
            get;
            set;
        }

        /// <summary>
        /// Enumerates the files in the transfer location referenced by this object.
        /// </summary>
        /// <param name="cancellationToken">CancellationToken to notify the method cancellation.</param>
        /// <returns>Enumerable list of TransferEntry objects found in the storage location referenced by this object.</returns>
        IEnumerable<TransferEntry> EnumerateLocation(CancellationToken cancellationToken);
    }
}
