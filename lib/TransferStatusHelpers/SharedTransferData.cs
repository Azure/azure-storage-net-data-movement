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

        public long ReadLength { get => Interlocked.Read(ref readLength); }
        public long WrittenLength { get => Interlocked.Read(ref writtenLength); }

        public int BlockSize { get; set; }

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

        public bool ContainsKey(long key) => this.data.ContainsKey(key);
        public bool IsEmpty { get => this.data.IsEmpty; }
        public void Clear() => this.data.Clear();
        public ICollection<TransferData> Values { get => this.data.Values; }
        public ICollection<long> Keys { get => this.data.Keys; }

        public event EventHandler<ValueChangeEventArgs<long>> TotalLengthChanged;
        public event EventHandler<TransferDataEventArgs> TransferDataAdded;
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
        public long Offset { get; set; }
        public bool Success { get; set; }
    }
}
