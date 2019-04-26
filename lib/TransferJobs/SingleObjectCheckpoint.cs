//------------------------------------------------------------------------------
// <copyright file="SingleObjectCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents checkpoint of a single transfer job, 
    /// includes position of transferred bytes and transfer window.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
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

#if !BINARY_SERIALIZATION
        [DataMember]
        private long entryTransferOffset = 0;

        [DataMember]
        private List<long> transferWindow = new List<long>();

        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            lock (this.TransferWindowLock)
            {
                entryTransferOffset = this.EntryTransferOffset;
                transferWindow = new List<long>(this.TransferWindow);
            }
        }

        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            // Constructors aren't called by DCS, so initialize non-serialized members here
            this.TransferWindowLock = new object();
            this.EntryTransferOffset = this.entryTransferOffset;
            this.TransferWindow = new List<long>(this.transferWindow);
        }
#endif

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
