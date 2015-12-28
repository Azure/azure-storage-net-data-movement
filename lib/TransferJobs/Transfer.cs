//------------------------------------------------------------------------------
// <copyright file="Transfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Base class for transfer operation.
    /// </summary>
    internal abstract class Transfer : ISerializable, IDisposable
    {
        private const string FormatVersionName = "Version";
        private const string SourceName = "Source";
        private const string DestName = "Dest";
        private const string TransferMethodName = "TransferMethod";
        private const string TransferProgressName = "Progress";

        /// <summary>
        /// Initializes a new instance of the <see cref="Transfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public Transfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
        {
            this.Source = source;
            this.Destination = dest;
            this.TransferMethod = transferMethod;
            this.ProgressTracker = new TransferProgressTracker();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Transfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected Transfer(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            string version = info.GetString(FormatVersionName);
            if (!string.Equals(Constants.FormatVersion, version, StringComparison.Ordinal))
            {
                throw new System.InvalidOperationException(
                    string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.DeserializationVersionNotMatchException,
                    "TransferJob",
                    version,
                    Constants.FormatVersion));
            }

            var serializableSourceLocation = (SerializableTransferLocation)info.GetValue(SourceName, typeof(SerializableTransferLocation));
            var serializableDestLocation = (SerializableTransferLocation)info.GetValue(DestName, typeof(SerializableTransferLocation));
            this.Source = serializableSourceLocation.Location;
            this.Destination = serializableDestLocation.Location;
            this.TransferMethod = (TransferMethod)info.GetValue(TransferMethodName, typeof(TransferMethod));
            this.ProgressTracker = (TransferProgressTracker)info.GetValue(TransferProgressName, typeof(TransferProgressTracker));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Transfer"/> class.
        /// </summary>
        protected Transfer(Transfer other)
        {
            this.Source = other.Source;
            this.Destination = other.Destination;
            this.TransferMethod = other.TransferMethod;
            this.ContentType = other.ContentType;
            this.ProgressTracker = other.ProgressTracker.Copy();
        }

        /// <summary>
        /// Gets source location for this transfer.
        /// </summary>
        public TransferLocation Source
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets destination location for this transfer.
        /// </summary>
        public TransferLocation Destination
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the transfer method used in this transfer.
        /// </summary>
        public TransferMethod TransferMethod
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets or sets the transfer context of this transfer.
        /// </summary>
        public TransferContext Context
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets content type to set to destination in uploading.
        /// </summary>
        public string ContentType
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets blob type of destination blob.
        /// </summary>
        public BlobType BlobType
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the progress tracker for this transfer.
        /// </summary>
        public TransferProgressTracker ProgressTracker
        {
            get;
            private set;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(FormatVersionName, Constants.FormatVersion, typeof(string));
            SerializableTransferLocation serializableSourceLocation = new SerializableTransferLocation(this.Source);
            SerializableTransferLocation serializableDestLocation = new SerializableTransferLocation(this.Destination);
            info.AddValue(SourceName, serializableSourceLocation, typeof(SerializableTransferLocation));
            info.AddValue(DestName, serializableDestLocation, typeof(SerializableTransferLocation));
            info.AddValue(TransferMethodName, this.TransferMethod);
            info.AddValue(TransferProgressName, this.ProgressTracker);
        }

        /// <summary>
        /// Execute the transfer asynchronously.
        /// </summary>
        /// <param name="scheduler">Transfer scheduler</param>
        /// <param name="cancellationToken">Token that can be used to cancel the transfer.</param>
        /// <returns>A task representing the transfer operation.</returns>
        public abstract Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken);

        public void UpdateTransferJobStatus(TransferJob transferJob, TransferJobStatus targetStatus)
        {
            lock (this.ProgressTracker)
            {
                switch (targetStatus)
                {
                    case TransferJobStatus.Transfer:
                    case TransferJobStatus.Monitor:
                        if (transferJob.Status == TransferJobStatus.Failed)
                        {
                            this.ProgressTracker.AddNumberOfFilesFailed(-1);
                        }

                        break;

                    case TransferJobStatus.Skipped:
                        this.ProgressTracker.AddNumberOfFilesSkipped(1);
                        break;

                    case TransferJobStatus.Finished:
                        this.ProgressTracker.AddNumberOfFilesTransferred(1);
                        break;

                    case TransferJobStatus.Failed:
                        this.ProgressTracker.AddNumberOfFilesFailed(1);
                        break;

                    case TransferJobStatus.NotStarted:
                    
                    default:
                        break;
                }

                transferJob.Status = targetStatus;
            }
        }

        /// <summary>
        /// Public dispose method to release all resources owned.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            // Nothing to dispose
        }
    }
}
