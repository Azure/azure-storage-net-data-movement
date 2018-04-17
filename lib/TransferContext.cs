//------------------------------------------------------------------------------
// <copyright file="TransferContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents the context for a transfer, and provides additional runtime information about its execution.
    /// </summary>
    public abstract class TransferContext
    {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        /// <summary>
        /// Callback used to force overwrite the destination without existence check. 
        /// It can be used when destination credentials only contains write permission.
        /// </summary>
        /// <param name="source">Instance of source used to overwrite the destination.</param>
        /// <param name="destination">Instance of destination to be overwritten.</param>
        /// <returns>True if the file should be overwritten; otherwise false.</returns>
        /// <remarks>
        /// Read permission is still required in destination credentials in serivce side copy for copy status monitoring.
        /// </remarks>
        public static async Task<bool> ForceOverwrite(object source, object destination)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            return true;
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferContext" /> class.
        /// </summary>
        protected TransferContext()
            :this(checkpoint: null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferContext" /> class.
        /// </summary>
        /// <param name="checkpoint">An <see cref="TransferCheckpoint"/> object representing the last checkpoint from which the transfer continues on.</param>
        protected TransferContext(TransferCheckpoint checkpoint)
        {
            this.LogLevel = OperationContext.DefaultLogLevel;

            if (checkpoint == null)
            {
#if BINARY_SERIALIZATION
                this.Checkpoint = new TransferCheckpoint();
#else
                this.Checkpoint = new TransferCheckpoint(other: null);
#endif
            }
            else
            {
                this.Checkpoint = checkpoint.Copy();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferContext" /> class.
        /// </summary>
        /// <param name="journalStream">The stream into which the transfer journal info will be written into. 
        /// It can resume the previours paused transfer from its journal stream.</param>
        protected TransferContext(Stream journalStream)
        {
            this.Checkpoint = new TransferCheckpoint(journalStream);
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
        /// Gets or sets the logging level to be used for the related transfer operation.
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
        /// Gets or sets the callback invoked to tell whether to overwrite an existing destination.
        /// </summary>
        public ShouldOverwriteCallbackAsync ShouldOverwriteCallbackAsync
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the callback invoked to set destination's attributes in memory. 
        /// The attributes set in this callback will be sent to azure storage service. 
        /// </summary>
        public SetAttributesCallbackAsync SetAttributesCallbackAsync
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the progress update handler.
        /// </summary>
        public IProgress<TransferStatus> ProgressHandler
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
        /// The event triggered when a file transfer is completed successfully.
        /// </summary>
        public event EventHandler<TransferEventArgs> FileTransferred;

        /// <summary>
        /// The event triggered when a file transfer is skipped.
        /// </summary>
        public event EventHandler<TransferEventArgs> FileSkipped;

        /// <summary>
        /// The event triggered when a file transfer is failed.
        /// </summary>
        public event EventHandler<TransferEventArgs> FileFailed;

        /// <summary>
        /// Gets the overall transfer progress.
        /// </summary>
        internal TransferProgressTracker OverallProgressTracker
        {
            get
            {
                return this.Checkpoint.TransferCollection.OverallProgressTracker;
            }
        }

        /// <summary>
        /// Gets the transfer checkpoint that tracks all transfers related to this transfer context.
        /// </summary>
        internal TransferCheckpoint Checkpoint
        {
            get;
            private set;
        }

        internal void OnTransferSuccess(TransferEventArgs eventArgs)
        {
            EventHandler<TransferEventArgs> handler = this.FileTransferred;
            if (handler != null)
            {
                handler(this, eventArgs);
            }
        }

        internal void OnTransferSkipped(TransferEventArgs eventArgs)
        {
            EventHandler<TransferEventArgs> handler = this.FileSkipped;
            if (handler != null)
            {
                handler(this, eventArgs);
            }
        }

        internal void OnTransferFailed(TransferEventArgs eventArgs)
        {
            EventHandler<TransferEventArgs> handler = this.FileFailed;
            if (handler != null)
            {
                handler(this, eventArgs);
            }
        }
    }
}
