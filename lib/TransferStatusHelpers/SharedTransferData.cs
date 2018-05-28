//------------------------------------------------------------------------------
// <copyright file="SharedTransferData.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;

    internal class SharedTransferData
    {
        private long totalLength = 0;
        private long readLength = 0;
        private long writtenLength = 0;
        private ConcurrentDictionary<long, TransferData> data = new ConcurrentDictionary<long, TransferData>();

        /// <summary>
        /// Gets or sets length of source.
        /// </summary>
        public long TotalLength
        {
            get { return this.totalLength; }
            set
            {
                if (value < ReadLength)
                {
                    throw new ArgumentOutOfRangeException(nameof(value));
                }

                // The order in which events are sent to the handler cannot be gaurenteed
                // but we use Interlocked.Exchange to ensure the values are accurate
                var old = Interlocked.Exchange(ref this.totalLength, value);

                var handler = TotalLengthChanged;
                if (handler != null)
                {
                    handler.Invoke(this, new ValueChangeEventArgs<long> {
                        Old = old,
                        New = value
                    });
                }
            }
        }

        /// <summary>
        /// Gets the amount of data put into this <c>SharedTransferData</c> by the reader
        /// </summary>
        public long ReadLength { get { return Interlocked.Read(ref readLength); } }

        /// <summary>
        /// Gets the amount of data taken out of this <c>SharedTransferData</c> by the writer
        /// </summary>
        [global::System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        public long WrittenLength { get { return Interlocked.Read(ref writtenLength); } }

        /// <summary>
        /// Gets the set block size for this transfer
        /// </summary>
        public int BlockSize { get; set; }

        /// <summary>
        /// Gets the memory chunks needed to hold <c>BlockSize</c> bytes of data
        /// </summary>
        public int MemoryChunksRequiredEachTime { get; set; }

        /// <summary>
        /// Gets or sets the job instance representing the transfer.
        /// </summary>
        public TransferJob TransferJob { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether should disable validation of content md5.
        /// The reader should get this value from source's <c>RequestOptions</c>,
        /// the writer should do or not do validation on content md5 according to this value.
        /// </summary>
        public bool DisableContentMD5Validation { get; set; }

        /// <summary>
        /// Gets or sets attribute for blob/azure file.
        /// </summary>
        public Attributes Attributes { get; set; }

        /// <summary>
        /// Attempt to add a new <c>TransferData</c> object to this dictionary-like store
        /// </summary>
        /// <param name="key">The start offset of the data</param>
        /// <param name="value">The <c>TransferData</c> object which holds the data</param>
        /// <returns>True if the <c>TransferData</c> was added. False otherwise</returns>
        public bool TryAdd(long key, TransferData value)
        {
            var success = this.data.TryAdd(key, value);

            if (success)
            {
                Interlocked.Add(ref this.readLength, value.Length);
                Debug.Assert(Interlocked.Read(ref readLength) <= TotalLength);
            }

            var handler = TransferDataAdded;
            if (handler != null)
            {
                handler.Invoke(this, new TransferDataEventArgs {
                    Data = value,
                    Offset = key,
                    Success = success
                });
            }
            return success;
        }

        /// <summary>
        /// Attempt remove a <c>TransferData</c> object from this dictionary-like store
        /// </summary>
        /// <param name="key">The start offset of the data</param>
        /// <param name="value">The output reference for the retreived transfer data</param>
        /// <returns>True is data was successfully removed. False otherwise</returns>
        public bool TryRemove(long key, out TransferData value)
        {
            var success = this.data.TryRemove(key, out value);

            if (success)
            {
                Interlocked.Add(ref this.writtenLength, value.Length);
                Debug.Assert(Interlocked.Read(ref writtenLength) <= TotalLength);
            }

            var handler = TransferDataRemoved;
            if (handler != null)
            {
                handler.Invoke(this, new TransferDataEventArgs {
                    Data = value,
                    Offset = key,
                    Success = success
                });
            }
            return success;
        }

        /// <summary>
        /// Checks if a start offset is in this dictionary-like store
        /// </summary>
        /// <param name="key">The start offset</param>
        /// <returns>True if data with that start offset is in the store</returns>
        public bool ContainsKey(long key) => this.data.ContainsKey(key);

        /// <summary>
        /// True if there is no transfer data in this object
        /// </summary>
        public bool IsEmpty { get { return this.data.IsEmpty; } }

        /// <summary>
        /// Removes all transfer data from this object
        /// </summary>
        public void Clear() => this.data.Clear();

        /// <summary>
        /// Gets a collection of <c>TransferData</c> objects stored
        /// </summary>
        public ICollection<TransferData> Values { get { return this.data.Values; } }

        /// <summary>
        /// Gets a collection of start offsets stored
        /// </summary>
        [global::System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        public ICollection<long> Keys { get { return this.data.Keys; } }

        /// <summary>
        /// Called when the <c>Length</c> property changes
        /// </summary>
        public event EventHandler<ValueChangeEventArgs<long>> TotalLengthChanged;

        /// <summary>
        /// Called when transfer data is added
        /// </summary>
        public event EventHandler<TransferDataEventArgs> TransferDataAdded;

        /// <summary>
        /// Called when transfer data is removed
        /// </summary>
        public event EventHandler<TransferDataEventArgs> TransferDataRemoved;
    }

    internal class ValueChangeEventArgs<T> : EventArgs
    {
        public T Old { get; set; }
        public T New { get; set; }
    }

    internal class TransferDataEventArgs : EventArgs
    {
        public TransferData Data { get; set; }

        [global::System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        public long Offset { get; set; }

        public bool Success { get; set; }
    }
}
