//------------------------------------------------------------------------------
// <copyright file="TransferCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents a checkpoint from which a transfer may be resumed and continue.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    public class TransferCheckpoint
#if BINARY_SERIALIZATION
        : ISerializable
#endif
    {
        private const string TransferCollectionName = "TransferCollection";

        private StreamJournal Journal = null;

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        internal TransferCheckpoint()
        {
            this.TransferCollection = new TransferCollection<Transfer>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="other">Another TransferCheckpoint object. </param>
        private TransferCheckpoint(TransferCheckpoint other)
        {
            this.TransferCollection = other.TransferCollection.Copy();
        }
#else
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="other">Another TransferCheckpoint object. </param>
        internal TransferCheckpoint(TransferCheckpoint other)
        {
            if (null == other)
            {
                this.TransferCollection = new TransferCollection<Transfer>();
            }
            else
            { 
                this.TransferCollection = other.TransferCollection.Copy();
            }
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="journalStream">Stream to write checkpoint journal to. </param>
        internal TransferCheckpoint(Stream journalStream)
        {
            this.TransferCollection = new TransferCollection<Transfer>();
            this.Journal = new StreamJournal(journalStream);
            Transfer transferInstance = this.Journal.Initialize();

            if (null != transferInstance)
            {
                this.TransferCollection.AddTransfer(transferInstance);
            }
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamJournal"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected TransferCheckpoint(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.TransferCollection = (TransferCollection<Transfer>)info.GetValue(TransferCollectionName, typeof(TransferCollection<Transfer>));
        }
#endif

        /// <summary>
        /// Gets that container that tracks all transfers associated with this transfer checkpoint
        /// </summary>

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        internal TransferCollection<Transfer> TransferCollection
        {
            get;
            private set;
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

            info.AddValue(TransferCollectionName, this.TransferCollection, typeof(TransferCollection<Transfer>));
        }
#endif

        /// <summary>
        /// Adds a transfer to the transfer checkpoint.
        /// </summary>
        /// <param name="transfer">The transfer to be kept track of.</param>
        internal void AddTransfer(Transfer transfer)
        {
            this.Journal?.AddTransfer(transfer);
            this.TransferCollection.AddTransfer(transfer);
        }

        /// <summary>
        /// Gets a transfer with the specified source location, destination location and transfer method.
        /// </summary>
        /// <param name="sourceLocation">Source location of the transfer.</param>
        /// <param name="destLocation">Destination location of the transfer.</param>
        /// <param name="transferMethod">Transfer method.</param>
        /// <returns>A transfer that matches the specified source location, destination location and transfer method; Or null if no matches.</returns>
        internal Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod)
        {
            return this.TransferCollection.GetTransfer(sourceLocation, destLocation, transferMethod);
        }

        /// <summary>
        /// Gets a static snapshot of this transfer checkpoint
        /// </summary>
        /// <returns>A snapshot of current transfer checkpoint</returns>
        internal TransferCheckpoint Copy()
        {
            return new TransferCheckpoint(this);
        }
    }
}
