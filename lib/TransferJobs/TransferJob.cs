//------------------------------------------------------------------------------
// <copyright file="TransferJob.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using System.Threading;

    /// <summary>
    /// Represents transfer of a single file/blob.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(AzureFileDirectoryLocation))]
    [KnownType(typeof(AzureFileLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
#endif // BINARY_SERIALIZATION
    internal class TransferJob
#if BINARY_SERIALIZATION
        : ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string SourceName = "Source";
        private const string DestName = "Dest";
        private const string CheckedOverwriteName = "CheckedOverwrite";
        private const string OverwriteName = "Overwrite";
        private const string CopyIdName = "CopyId";
        private const string CheckpointName = "Checkpoint";
        private const string StatusName = "Status";
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferJob"/> class.
        /// </summary>
        /// <param name="transfer">Transfer object.</param>
        public TransferJob(Transfer transfer)
        {
            this.Transfer = transfer;

            this.CheckPoint = new SingleObjectCheckpoint();
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferJob"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected TransferJob(SerializationInfo info, StreamingContext context)
        {
            if (info.GetBoolean(CheckedOverwriteName))
            {
                this.Overwrite = info.GetBoolean(OverwriteName);
            }
            else
            {
                this.Overwrite = null;
            }

            this.CopyId = info.GetString(CopyIdName);
            this.CheckPoint = (SingleObjectCheckpoint)info.GetValue(CheckpointName, typeof(SingleObjectCheckpoint));
            this.Status = (TransferJobStatus)info.GetValue(StatusName, typeof(TransferJobStatus));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferJob"/> class.
        /// </summary>
        /// <param name="other">The other transfer job to copy properties.</param>
        private TransferJob(TransferJob other)
        {
            this.Overwrite = other.Overwrite;
            this.CopyId = other.CopyId;
            this.CheckPoint = other.CheckPoint.Copy();
            this.Status = other.Status;
        }

        public ReaderWriterLockSlim ProgressUpdateLock
        {
            get;
            set;
        }

        /// <summary>
        /// Gets source location for this transfer job.
        /// </summary>
        public TransferLocation Source
        {
            get
            {
                return this.Transfer.Source;
            }
        }

        /// <summary>
        /// Gets destination location for this transfer job.
        /// </summary>
        public TransferLocation Destination
        {
            get
            {
                return this.Transfer.Destination;
            }
        }

        /// <summary>
        /// Gets or sets the overwrite flag.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public bool? Overwrite
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets ID for the asynchronous copy operation.
        /// </summary>
        /// <value>ID for the asynchronous copy operation.</value>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public string CopyId
        {
            get;
            set;
        }

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public TransferJobStatus Status
        {
            get;
            set;
        }

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public SingleObjectCheckpoint CheckPoint
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the parent transfer of this transfer job
        /// </summary>
        public Transfer Transfer
        {
            get;
            set;
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

            info.AddValue(CheckedOverwriteName, this.Overwrite.HasValue);
            if (this.Overwrite.HasValue)
            {
                info.AddValue(OverwriteName, this.Overwrite.Value);
            }

            info.AddValue(CopyIdName, this.CopyId, typeof(string));
            info.AddValue(CheckpointName, this.CheckPoint, typeof(SingleObjectCheckpoint));
            info.AddValue(StatusName, this.Status, typeof(TransferJobStatus));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets a copy of this transfer job.
        /// </summary>
        /// <returns>A copy of current transfer job</returns>
        public TransferJob Copy()
        {
            return new TransferJob(this);
        }
    }
}
