//------------------------------------------------------------------------------
// <copyright file="TransferCollection.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Globalization;
    using System.Runtime.Serialization;
    using TransferKey = System.Tuple<TransferLocation, TransferLocation>;

    /// <summary>
    /// A collection of transfers.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
    [KnownType(typeof(DirectoryTransfer))]
    [KnownType(typeof(SingleObjectTransfer))]
#endif // BINARY_SERIALIZATION
    internal class TransferCollection<T>
#if BINARY_SERIALIZATION
        : ISerializable
        where T : Transfer
#endif // BINARY_SERIALIZATION
    {
        /// <summary>
        /// Serialization field name for single object transfers.
        /// </summary>
        private const string SingleObjectTransfersName = "SingleObjectTransfers";

        /// <summary>
        /// Serialization field name for directory transfers.
        /// </summary>
        private const string DirectoryTransfersName = "DirectoryTransfers";

        /// <summary>
        /// All transfers in the collection.
        /// </summary>
        private ConcurrentDictionary<TransferKey, Transfer> transfers = new ConcurrentDictionary<TransferKey, Transfer>();

        /// <summary>
        /// Overall transfer progress tracker.
        /// </summary>
        private TransferProgressTracker overallProgressTracker = new TransferProgressTracker();

#if BINARY_SERIALIZATION

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCollection{T}"/> class.
        /// </summary>
        internal TransferCollection()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCollection{T}"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected TransferCollection(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            int transferCount = info.GetInt32(SingleObjectTransfersName);

            for (int i = 0; i < transferCount; ++i)
            {
                this.AddTransfer((T)info.GetValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", SingleObjectTransfersName, i), typeof(SingleObjectTransfer)));
            }

            transferCount = info.GetInt32(DirectoryTransfersName);
            for (int i = 0; i < transferCount; ++i)
            {
                this.AddTransfer((T)info.GetValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", DirectoryTransfersName, i), typeof(DirectoryTransfer)));
            }

            foreach (Transfer transfer in this.transfers.Values)
            {
                this.OverallProgressTracker.AddProgress(transfer.ProgressTracker);
            }
        }
#endif // BINARY_SERIALIZATION

#region Serialization helpers

#if !BINARY_SERIALIZATION
        [DataMember]
        private Transfer[] serializedTransfers;

        /// <summary>
        /// Initializes a deserialized TransferCollection (by rebuilding the the transfer
        /// dictionary and progress tracker)
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            // DCS doesn't invoke ctors, so all initialization must be done here
            transfers = new ConcurrentDictionary<TransferKey, Transfer>();
            overallProgressTracker = new TransferProgressTracker();

            foreach (Transfer t in serializedTransfers)
            {
                this.AddTransfer(t);
            }

            foreach (Transfer transfer in this.transfers.Values)
            {
                this.OverallProgressTracker.AddProgress(transfer.ProgressTracker);
            }
        }

        /// <summary>
        /// Serializes the object by storing the trasnfers in a more DCS-friendly format
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            serializedTransfers = this.transfers.Select(kv => kv.Value).Where(t => t != null).ToArray();
        }
#endif //!BINARY_SERIALIZATION
#endregion // Serialization helpers

        /// <summary>
        /// Gets the number of transfers currently in the collection.
        /// </summary>
        public int Count
        {
            get
            {
                return this.transfers.Count;
            }
        }

        /// <summary>
        /// Gets the overall transfer progress.
        /// </summary>
        public TransferProgressTracker OverallProgressTracker
        {
            get
            {
                return this.overallProgressTracker;
            }
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the checkpoint.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            List<SingleObjectTransfer> singleObjectTransfers = new List<SingleObjectTransfer>();
            List<DirectoryTransfer> directoryTransfers = new List<DirectoryTransfer>();
            foreach (var kv in this.transfers)
            {
                SingleObjectTransfer transfer = kv.Value as SingleObjectTransfer;
                if (transfer != null)
                {
                    singleObjectTransfers.Add(transfer);
                    continue;
                }

                DirectoryTransfer transfer2 = kv.Value as DirectoryTransfer;
                if (transfer2 != null)
                {
                    directoryTransfers.Add(transfer2);
                    continue;
                }
            }

            info.AddValue(SingleObjectTransfersName, singleObjectTransfers.Count);

            for (int i = 0; i < singleObjectTransfers.Count; ++i)
            {
                info.AddValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", SingleObjectTransfersName, i), singleObjectTransfers[i], typeof(SingleObjectTransfer));
            }

            info.AddValue(DirectoryTransfersName, directoryTransfers.Count);

            for (int i = 0; i < directoryTransfers.Count; ++i)
            {
                info.AddValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", DirectoryTransfersName, i), directoryTransfers[i], typeof(DirectoryTransfer));
            }
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Adds a transfer.
        /// </summary>
        /// <param name="transfer">The transfer to be added.</param>
        /// <param name="updateProgress">Whether or not to update collection's progress with the subtransfer's.</param>
#if DOTNET5_4
        public void AddTransfer(Transfer transfer, bool updateProgress = true)
#else
        public void AddTransfer(T transfer, bool updateProgress = true)
#endif
        {
            transfer.ProgressTracker.Parent = this.OverallProgressTracker;

            if (updateProgress)
            {
                this.overallProgressTracker.AddProgress(transfer.ProgressTracker);
            }

            bool unused = this.transfers.TryAdd(new TransferKey(transfer.Source, transfer.Destination), transfer);
            Debug.Assert(unused, "Transfer with the same source and destination already exists");
        }

        /// <summary>
        /// Remove a transfer.
        /// </summary>
        /// <param name="transfer">Transfer to be removed</param>
        /// <returns>True if the transfer is removed successfully, false otherwise.</returns>
        public bool RemoveTransfer(Transfer transfer)
        {
            Transfer unused = null;
            if (this.transfers.TryRemove(new TransferKey(transfer.Source, transfer.Destination), out unused))
            {
                transfer.ProgressTracker.Parent = null;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets a transfer with the specified source location, destination location and transfer method.
        /// </summary>
        /// <param name="sourceLocation">Source location of the transfer.</param>
        /// <param name="destLocation">Destination location of the transfer.</param>
        /// <param name="transferMethod">Transfer method.</param>
        /// <returns>A transfer that matches the specified source location, destination location and transfer method; Or null if no matches.</returns>
        public Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod)
        {
            Transfer transfer = null;
            if (this.transfers.TryGetValue(new TransferKey(sourceLocation, destLocation), out transfer))
            {
                if (transfer.TransferMethod == transferMethod)
                {
                    return transfer;
                }
            }

            return null;
        }

        /// <summary>
        /// Get an enumerable object for all tansfers in this TransferCollection.
        /// </summary>
        /// <returns>An enumerable object for all tansfers in this TransferCollection.</returns>
        public IEnumerable<Transfer> GetEnumerator()
        {
            return this.transfers.Values;
        }

        /// <summary>
        /// Gets a static snapshot of this transfer checkpoint
        /// </summary>
        /// <returns>A snapshot of current transfer checkpoint</returns>
#if DOTNET5_4
        public TransferCollection<T> Copy()
        {
            TransferCollection<T> copyObj = new TransferCollection<T>();
            foreach (var kv in this.transfers)
            {
                Transfer transfer = kv.Value as Transfer;
                copyObj.AddTransfer((Transfer)transfer.Copy());
            }

            return copyObj;
        }
#else
        public TransferCollection<T> Copy()
        {
            TransferCollection<T> copyObj = new TransferCollection<T>();
            foreach (var kv in this.transfers)
            {
                var transfer = kv.Value;
                copyObj.AddTransfer((T)transfer.Copy());
            }

            return copyObj;
        }
#endif
    }
}
