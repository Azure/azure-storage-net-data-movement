//------------------------------------------------------------------------------
// <copyright file="SingleTransferContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System.IO;

    /// <summary>
    /// Represents the context for a single transfer, and provides additional runtime information about its execution.
    /// </summary>
    public class SingleTransferContext : TransferContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SingleTransferContext" /> class.
        /// </summary>
        public SingleTransferContext()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleTransferContext" /> class.
        /// </summary>
        /// <param name="checkpoint">An <see cref="TransferCheckpoint"/> object representing the last checkpoint from which the transfer continues on.</param>
        public SingleTransferContext(TransferCheckpoint checkpoint)
            :base(checkpoint)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleTransferContext" /> class.
        /// </summary>
        /// <param name="journalStream">The stream into which the transfer journal info will be written into.
        /// It can resume the previous paused transfer from its journal stream.</param>
        public SingleTransferContext(Stream journalStream)
            :base(journalStream)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleTransferContext" /> class.
        /// </summary>
        /// <param name="journalStream">The stream into which the transfer journal info will be written into.
        /// It can resume the previous paused transfer from its journal stream.</param>
        /// <param name="disableJournalValidation">A flag that indicates whether to validate an assembly version serialized in a journal stream or not.</param>
        public SingleTransferContext(Stream journalStream, bool disableJournalValidation)
            : base(journalStream, disableJournalValidation)
        {
        }
    }
}
