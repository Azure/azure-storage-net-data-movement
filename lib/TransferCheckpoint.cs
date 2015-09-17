//------------------------------------------------------------------------------
// <copyright file="TransferCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using TransferKey = System.Tuple<TransferLocation, TransferLocation>;

    /// <summary>
    /// Represents a checkpoint from which a transfer may be resumed and continue.
    /// </summary>
    [Serializable]
    public class TransferCheckpoint : ISerializable
    {
        private const string SingleObjectTransfersName = "SingleObjectTransfers";

        /// <summary>
        /// Transfers associated with this transfer checkpoint.
        /// </summary>
        private ConcurrentDictionary<TransferKey, Transfer> transfers = new ConcurrentDictionary<TransferKey, Transfer>();

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        internal TransferCheckpoint()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected TransferCheckpoint(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            var singleObjectTransfers = (List<SingleObjectTransfer>)info.GetValue(SingleObjectTransfersName, typeof(List<SingleObjectTransfer>));
            foreach(var transfer in singleObjectTransfers)
            {
                this.AddTransfer(transfer);
            }
        }


        /// <summary>
        /// Gets a list of all transfers
        /// </summary>
        internal ICollection<Transfer> AllTransfers
        {
            get
            {
                return this.transfers.Values;
            }
        }

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
            foreach(var kvPair in this.transfers)
            {
                SingleObjectTransfer transfer = kvPair.Value as SingleObjectTransfer;
                if (transfer != null)
                {
                    singleObjectTransfers.Add(transfer);
                }
            }

            info.AddValue(SingleObjectTransfersName, singleObjectTransfers, typeof(List<SingleObjectTransfer>));
        }

        /// <summary>
        /// Adds a transfer to the transfer checkpoint.
        /// </summary>
        /// <param name="transfer">The transfer to be kept track of.</param>
        internal void AddTransfer(Transfer transfer)
        {
            this.transfers.TryAdd(new TransferKey(transfer.Source, transfer.Destination), transfer);
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
        /// Gets a static snapshot of this transfer checkpoint
        /// </summary>
        /// <returns>A snapshot of current transfer checkpoint</returns>
        internal TransferCheckpoint Copy()
        {
            TransferCheckpoint copyObj = new TransferCheckpoint();
            foreach (var kvPair in this.transfers)
            {
                SingleObjectTransfer transfer = kvPair.Value as SingleObjectTransfer;
                if (transfer != null)
                {
                    copyObj.AddTransfer(transfer.Copy());
                }
            }

            return copyObj;
        }
    }
}
