//------------------------------------------------------------------------------
// <copyright file="TransferContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;

    /// <summary>
    /// Represents the context for a transfer, and provides additional runtime information about its execution.
    /// </summary>
    public class TransferContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferContext" /> class.
        /// </summary>
        public TransferContext()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferContext" /> class.
        /// </summary>
        /// <param name="checkpoint">An <see cref="TransferCheckpoint"/> object representing the last checkpoint from which the transfer continues on.</param>
        public TransferContext(TransferCheckpoint checkpoint)
        {
            if (checkpoint == null)
            {
                this.Checkpoint = new TransferCheckpoint();
            }
            else
            {
                this.Checkpoint = checkpoint.Copy();
            }

            this.OverallProgressTracker = new TransferProgressTracker();
            foreach(Transfer transfer in this.Checkpoint.AllTransfers)
            {
                this.OverallProgressTracker.AddBytesTransferred(transfer.ProgressTracker.BytesTransferred);
                this.OverallProgressTracker.AddNumberOfFilesTransferred(transfer.ProgressTracker.NumberOfFilesTransferred);
                this.OverallProgressTracker.AddNumberOfFilesSkipped(transfer.ProgressTracker.NumberOfFilesSkipped);
                this.OverallProgressTracker.AddNumberOfFilesFailed(transfer.ProgressTracker.NumberOfFilesFailed);
            }
        }

        /// <summary>
        /// Gets or sets the client request id.
        /// </summary>
        /// <value>A string containing the client request id.</value>
        /// <remarks>
        /// Setting this property modifies all the requests involved in the related transfer operation to include the the HTTP <i>x-ms-client-request-id</i> header.
        /// </remarks>
        public string ClientRequestId
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the logging level to be used for the related tranfer operation.
        /// </summary>
        /// <value>A value of type <see cref="Microsoft.WindowsAzure.Storage.LogLevel"/> that specifies which events are logged for the related transfer operation.</value>
        public LogLevel LogLevel
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the last checkpoint of the transfer.
        /// </summary>
        public TransferCheckpoint LastCheckpoint
        {
            get
            {
                return this.Checkpoint.Copy();
            }
        }

        /// <summary>
        /// Callback invoked to tell whether to overwrite an existing destination.
        /// </summary>
        public OverwriteCallback OverwriteCallback
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the progress update handler.
        /// </summary>
        public IProgress<TransferProgress> ProgressHandler
        {
            get
            {
                return this.OverallProgressTracker.ProgressHandler;
            }
            set
            {
                this.OverallProgressTracker.ProgressHandler = value;
            }
        }

        /// <summary>
        /// Gets the overall transfer progress.
        /// </summary>
        internal TransferProgressTracker OverallProgressTracker
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the transfer checkpoint that tracks all transfers related to this transfer context.
        /// </summary>
        internal TransferCheckpoint Checkpoint
        {
            get;
            private set;
        }
    }
}
