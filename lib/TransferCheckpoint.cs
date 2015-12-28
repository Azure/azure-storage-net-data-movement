//------------------------------------------------------------------------------
// <copyright file="TransferCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents a checkpoint from which a transfer may be resumed and continue.
    /// </summary>
    [Serializable]
    public class TransferCheckpoint
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        internal TransferCheckpoint()
        {
            this.TransferCollection = new TransferCollection();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="other">Another TransferCheckpoint object. </param>
        private TransferCheckpoint(TransferCheckpoint other)
        {
            this.TransferCollection = other.TransferCollection.Copy();
        }

        /// <summary>
        /// Gets that container that tracks all transfers associated with this transfer checkpoint
        /// </summary>
        internal TransferCollection TransferCollection
        {
            get;
            private set;
        }

        /// <summary>
        /// Adds a transfer to the transfer checkpoint.
        /// </summary>
        /// <param name="transfer">The transfer to be kept track of.</param>
        internal void AddTransfer(Transfer transfer)
        {
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
