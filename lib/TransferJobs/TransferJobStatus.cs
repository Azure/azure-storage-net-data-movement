//------------------------------------------------------------------------------
// <copyright file="TransferJobStatus.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Status for TransferEntry.
    /// NotStarted -> Skipped
    ///            -> Transfer -> [Monitor ->] Finished.
    ///                                        Failed.
    /// </summary>
    internal enum TransferJobStatus
    {
        /// <summary>
        /// Transfer is not started.
        /// </summary>
        NotStarted,

        /// <summary>
        /// The transfer should not be done by customer's choice.
        /// This is only used when the transfer instance is created from a directory transfer.
        /// </summary>
        NotTransfer,

        /// <summary>
        /// Transfer is skipped
        /// </summary>
        Skipped,

        /// <summary>
        /// Transfer file.
        /// </summary>
        Transfer,

        /// <summary>
        /// Monitor transfer process.
        /// </summary>
        Monitor,

        /// <summary>
        /// Transfer is finished successfully.
        /// </summary>
        Finished,

        /// <summary>
        /// Transfer is failed.
        /// </summary>
        Failed,
    }
}
