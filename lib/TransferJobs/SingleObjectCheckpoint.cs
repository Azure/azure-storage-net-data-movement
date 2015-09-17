//------------------------------------------------------------------------------
// <copyright file="SingleObjectCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents checkpoint of a single transfer job, 
    /// includes position of transferred bytes and transfer window.
    /// </summary>
    [Serializable]
    internal sealed class SingleObjectCheckpoint
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SingleObjectCheckpoint"/> class.
        /// </summary>
        /// <param name="entryTransferOffset">Transferred offset of this transfer entry.</param>
        /// <param name="transferWindow">Transfer window of this transfer entry.</param>
        public SingleObjectCheckpoint(long entryTransferOffset, IEnumerable<long> transferWindow)
        {
            this.EntryTransferOffset = entryTransferOffset;
            if (null != transferWindow)
            {
                this.TransferWindow = new List<long>(transferWindow);
            }
            else
            {
                this.TransferWindow = new List<long>(Constants.MaxCountInTransferWindow);
            }

            this.TransferWindowLock = new object();
        }

        public SingleObjectCheckpoint()
            : this(0, null)
        {
        }

        /// <summary>
        /// Gets or sets transferred offset of this transfer entry.
        /// </summary>
        /// <value>Transferred offset of this transfer entry.</value>
        public long EntryTransferOffset
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets transfer window of this transfer entry.
        /// </summary>
        /// <value>Transfer window of this transfer entry.</value>
        public List<long> TransferWindow
        {
            get;
            set;
        }

        public object TransferWindowLock
        {
            get;
            private set;
        }

        public SingleObjectCheckpoint Copy()
        {
            SingleObjectCheckpoint copyObj = new SingleObjectCheckpoint();
            lock (this.TransferWindowLock)
            {
                copyObj.EntryTransferOffset = this.EntryTransferOffset;
                copyObj.TransferWindow = new List<long>(this.TransferWindow);
            }

            return copyObj;
        }
    }
}
