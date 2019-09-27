//------------------------------------------------------------------------------
// <copyright file="Transfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Base class for transfer operation.
    /// </summary>
#if !BINARY_SERIALIZATION
    [DataContract]
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(AzureFileDirectoryLocation))]
    [KnownType(typeof(AzureFileLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
    [KnownType(typeof(SingleObjectTransfer))]
    [KnownType(typeof(DirectoryTransfer))]
    [KnownType(typeof(SubDirectoryTransfer))]
#endif
    internal abstract class Transfer : JournalItem, IDisposable
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string FormatVersionName = "Version";
        private const string SourceName = "Source";
        private const string DestName = "Dest";
        private const string TransferMethodName = "TransferMethod";
        private const string TransferProgressName = "Progress";

        // Currently, we have two ways to persist the transfer instance:
        // 1. User can persist a TransferCheckpoint instance with all transfer instances in it.
        // 2. User can input a stream to TransferCheckpoint that DMLib will persistant ongoing transfer instances to the stream.
        // 2# solution is used to transfer large amount of files without occupying too much memory. 
        // With this solution, 
        // a. when persisting a DirectoryTransfer, we don't save its subtransfers with it, instead we'll allocate a new 
        //    transfer chunk for each subtransfer. 
        // b. We don't persist its TransferProgressTracker with Transfer instance, instead we save the TransferProgressTracker to a separate place.
        // Please reference to explaination in StreamJournal for details.

#if !BINARY_SERIALIZATION
        [DataMember]
        private TransferProgressTracker progressTracker;
#endif

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
            this.OriginalFormatVersion = Constants.FormatVersion;
        }

#if BINARY_SERIALIZATION
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

            if (null == context.Context || !(context.Context is StreamJournal))
            {
                this.ProgressTracker = (TransferProgressTracker)info.GetValue(TransferProgressName, typeof(TransferProgressTracker));
            }
            else
            {
                this.ProgressTracker = new TransferProgressTracker();
            }
        }
#endif // BINARY_SERIALIZATION

#if !BINARY_SERIALIZATION
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            if (!IsStreamJournal)
            {
                this.progressTracker = this.ProgressTracker;
            }
        }

        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            if (!string.Equals(Constants.FormatVersion, OriginalFormatVersion, StringComparison.Ordinal))
            {
                throw new System.InvalidOperationException(
                    string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.DeserializationVersionNotMatchException,
                    "TransferJob",
                    OriginalFormatVersion,
                    Constants.FormatVersion));
            }

            if (!IsStreamJournal)
            {
                this.ProgressTracker = this.progressTracker;
            }
            else
            {
                this.ProgressTracker = new TransferProgressTracker();
            }

            if (this.Source != null)
            {
                this.Source.IsInstanceInfoFetched = null;
            }

            if (this.Destination != null)
            {
                this.Destination.IsInstanceInfoFetched = null;
            }
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="Transfer"/> class.
        /// </summary>
        protected Transfer(Transfer other)
        {
            this.Source = other.Source;
            this.Destination = other.Destination;
            this.TransferMethod = other.TransferMethod;
            this.OriginalFormatVersion = other.OriginalFormatVersion;
            this.PreserveSMBAttributes = other.PreserveSMBAttributes;
        }

        /// Used to ensure that deserialized transfers are only used
        /// in scenarios with the same format version they were serialized with.
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private string OriginalFormatVersion { get; set; }

        /// <summary>
        /// Gets source location for this transfer.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public TransferLocation Source
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets destination location for this transfer.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public TransferLocation Destination
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the transfer method used in this transfer.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public TransferMethod TransferMethod
        {
            get;
            private set;
        }

#if !BINARY_SERIALIZATION
        /// <summary>
        /// Gets or sets a variable to indicate whether the transfer will be saved to a streamed journal.
        /// </summary>
        [DataMember]
        public bool IsStreamJournal { get; set; }
#endif

        /// <summary>
        /// Gets or sets the transfer context of this transfer.
        /// </summary>
        public virtual TransferContext Context
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
        /// Gets or sets a flag that indicates whether to preserve SMB attributes 
        /// during transferring between local file to Azure File Service.
        /// </summary>
        public bool PreserveSMBAttributes
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
            protected set;
        }

#if BINARY_SERIALIZATION
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

            if (null == context.Context || !(context.Context is StreamJournal))
            {
                info.AddValue(TransferProgressName, this.ProgressTracker);
            }
        }
#endif // BINARY_SERIALIZATION

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
                            UpdateProgress(transferJob, () => this.ProgressTracker.AddNumberOfFilesFailed(-1));
                        }

                        break;

                    case TransferJobStatus.Skipped:
                        UpdateProgress(transferJob, () => this.ProgressTracker.AddNumberOfFilesSkipped(1));
                        break;

                    case TransferJobStatus.Finished:
                        UpdateProgress(transferJob, () => this.ProgressTracker.AddNumberOfFilesTransferred(1));
                        break;

                    case TransferJobStatus.Failed:
                        UpdateProgress(transferJob, () => this.ProgressTracker.AddNumberOfFilesFailed(1));
                        break;

                    case TransferJobStatus.NotStarted:
                    
                    default:
                        break;
                }

                transferJob.Status = targetStatus;
            }

            transferJob.Transfer.UpdateJournal();
        }

        public abstract Transfer Copy();

        public void UpdateJournal()
        {
            this.Journal?.UpdateJournalItem(this);
        }

        private static void UpdateProgress(TransferJob job, Action updateAction)
        {
            try
            {
                job.ProgressUpdateLock?.EnterReadLock();
                updateAction();
            }
            finally
            {
                job.ProgressUpdateLock?.ExitReadLock();
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
