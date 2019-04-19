//------------------------------------------------------------------------------
// <copyright file="ErrorEntry.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// Class inherit from TransferEntry to indicate transfer enumeration failures.
    /// </summary>
    internal class ErrorEntry : TransferEntry
    {
        /// <summary>
        /// Exception received during transfer enumeration.
        /// </summary>
        public readonly TransferException Exception;

        /// <summary>
        /// Initializes a new instance of the <see cref="ErrorEntry" /> class.
        /// </summary>
        /// <param name="ex">Exception to store.</param>
        public ErrorEntry(TransferException ex)
            : base(null, null)
        {
            this.Exception = ex;
        }

        public override bool IsDirectory { get => false; }
    }
}
