//------------------------------------------------------------------------------
// <copyright file="DirectoryTransferContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

using System;

namespace Microsoft.Azure.Storage.DataMovement
{
    using System.IO;

    /// <summary>
    /// Represents the context for a directory transfer, and provides additional runtime information about its execution.
    /// </summary>
    public class DirectoryTransferContext : TransferContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransferContext" /> class.
        /// </summary>
        public DirectoryTransferContext()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransferContext" /> class.
        /// </summary>
        /// <param name="checkpoint">An <see cref="TransferCheckpoint"/> object representing the last checkpoint from which the transfer continues on.</param>
        public DirectoryTransferContext(TransferCheckpoint checkpoint)
            :base(checkpoint)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransferContext" /> class.
        /// </summary>
        /// <param name="journalStream">The stream into which the transfer journal info will be written into.
        /// It can resume the previous paused transfer from its journal stream.</param>
        public DirectoryTransferContext(Stream journalStream)
            :base(journalStream)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransferContext" /> class.
        /// </summary>
        /// <param name="journalStream">The stream into which the transfer journal info will be written into.
        /// It can resume the previous paused transfer from its journal stream.</param>
        /// <param name="disableJournalValidation">A flag that indicates whether to validate an assembly version serialized in a journal stream or not.</param>
        public DirectoryTransferContext(Stream journalStream, bool disableJournalValidation)
            : base(journalStream, disableJournalValidation)
        {
        }

        /// <summary>
        /// Gets or sets the callback invoked to tell whether a transfer should be done.
        /// </summary>
        public ShouldTransferCallbackAsync ShouldTransferCallbackAsync
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the time after which a transfer is considered as stuck.
        /// </summary>
        public TimeSpan? TransferStuckTimeout
        {
            get;
            set;
        }
    }
}
