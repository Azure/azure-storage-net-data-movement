//------------------------------------------------------------------------------
// <copyright file="StreamJournal.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
#if BINARY_SERIALIZATION
    using System.Runtime.Serialization.Formatters.Binary;
#endif
    using System.Text;

    internal class StreamJournal
    {
        //------------------------------------------------------------------------------------------------------
        // 0-255: Journal format version string: Assembly name + version
        // 256 - 511: Journal head, keep the list of used chunks for transfer instances and free chunks.
        // 512- : Chunks for transfer instances
        // Size of each chunk is 10K, 9K for transfer instance, 1K for progress tracker of the transfer instance.
        // In the journal, it only allows one base transfer, which means user can only add one transfer to the checkpoint using stream journal.
        // A base transfer can be a SingleObjectTransfer or a MultipleObjectTransfer, if it's a MultipleObjectTransfer,
        // there could be multiple subtransfers, each subtransfer is a SingleObjectTransfer.
        //------------------------------------------------------------------------------------------------------
        
        /// <summary>
        /// Size for one saved transfer instance in the journal stream.
        /// To reuse space transfer instances in journal stream to avoid occupy too much disks when transferring
        /// large amount of files, it allocates fixed size spaces for one transfer instance in the stream.
        /// </summary>
        private const int TransferChunkSize = 10 * 1024;

        /// <summary>
        /// For each transfer instance, it saves the transfer object itself and the transfer's ProgressTracker in the journal.
        /// This is size to allocated for the transfer object itself.
        /// </summary>
        private const int TransferItemContentSize = 9 * 1024;

        /// <summary>
        /// It keeps a list of used transfer chunks and a list free transfers in the journal stream,
        /// journal head keeps the heads and tails for these two lists.
        /// </summary>
        private const int JournalHeadOffset = 256;

        /// <summary>
        /// Offset in stream for the beginning to persistant transfer instance.
        /// </summary>
        private const int contentOffset = 512;

        public int ContentOffset
        {
            get
            {
                return contentOffset + this.baseTransferSize + (TransferChunkSize - TransferItemContentSize);
            }
        }

        /// <summary>
        /// In the journal, it only allows one base transfer, which means user can only add one transfer to the checkpoint using stream journal.
        /// A base transfer can be a SingleObjectTransfer or a MultipleObjectTransfer, if it's a MultipleObjectTransfer,
        /// there could be multiple subtransfers, each subtransfer is a SingleObjectTransfer.
        /// </summary>
        private Transfer baseTransfer = null;
        private int baseTransferSize = 0;

        private Stream stream;

        private string absoluteDirectoryPath = null;

        public string DirectoryPath
        {
            get
            {
                return absoluteDirectoryPath;
            }
            set
            {
                this.absoluteDirectoryPath = value;
            }
        }
        /// <summary>
        /// Lock for reading/writing from/to the journal stream.
        /// </summary>
        private object journalLock = new object();

#if BINARY_SERIALIZATION
        private IFormatter formatter = new BinaryFormatter();
#else 
        /// <summary>
        /// Buffer for serializerStream.
        /// </summary>
        private byte[] serializerBuffer = null;

        /// <summary>
        /// DataContractSerializer validates source file's schema when deserializes from it, 
        /// while DMLib saves the transfer instances in a binary file.
        /// We uses a separate stream to read the whole XML content from stream journal for DataContractSerializer.
        /// </summary>
        private MemoryStream serializerStream = null;

        private DataContractSerializer stringSerializer = new DataContractSerializer(typeof(string));
        private DataContractSerializer transferSerializer = new DataContractSerializer(typeof(Transfer));
        private DataContractSerializer progressCheckerSerializer = new DataContractSerializer(typeof(TransferProgressTracker));
#endif        

        long usedChunkHead = 0;
        long usedChunkTail = 0;
        long freeChunkHead = 0;
        long freeChunkTail = 0;

        /// <summary>
        /// This is the granularity to allocation memory buffer.
        /// 4K buffer would be enough for most of the TransferEntry serialization.
        /// </summary>
        private const int BufferSizeGranularity = 4096;

        /// <summary>
        /// Buffer used to read from or write to journal.
        /// </summary>
        private byte[] memoryBuffer = new byte[BufferSizeGranularity];

        public StreamJournal(Stream journal)
        {
            stream = journal;
#if BINARY_SERIALIZATION
            formatter.Context = new StreamingContext(formatter.Context.State, this);
#else
            serializerBuffer = new byte[TransferChunkSize];
            serializerStream = new MemoryStream(serializerBuffer);
#endif
        }

        public Transfer Initialize()
        {
            lock (this.journalLock)
            {
                if (stream.Length == 0)
                {
                    this.stream.Position = 0;

#if BINARY_SERIALIZATION
                    this.formatter.Serialize(stream, Constants.FormatVersion);
#else
                    this.WriteObject(this.stringSerializer, Constants.FormatVersion);
#endif

                    this.WriteJournalHead();

                    this.stream.Flush();
                    return null;
                }
                else
                {
                    stream.Position = 0;
#if BINARY_SERIALIZATION
                    string version = (string)formatter.Deserialize(this.stream);
#else
                    string version = (string)this.ReadObject(this.stringSerializer);
#endif

                    if (!string.Equals(version, Constants.FormatVersion, StringComparison.Ordinal))
                    {
                        throw new System.InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.DeserializationVersionNotMatchException,
                            "Journal",
                            version,
                            Constants.FormatVersion));
                    }

                    this.stream.Position = JournalHeadOffset;
                    this.usedChunkHead = this.ReadLong();
                    this.usedChunkTail = this.ReadLong();
                    this.freeChunkHead = this.ReadLong();
                    this.freeChunkTail = this.ReadLong();

                    // absolute path
                    this.stream.Position = contentOffset;
                    this.baseTransferSize = this.ReadInt();
                    this.stream.Position = contentOffset + sizeof(int);

#if BINARY_SERIALIZATION
                    this.baseTransfer = (Transfer)this.formatter.Deserialize(this.stream);
#else
                    this.baseTransfer = this.ReadObject(this.transferSerializer) as Transfer;
#endif
                    this.baseTransfer.StreamJournalOffset = contentOffset + sizeof(int);
                    this.baseTransfer.Journal = this;

                    if (baseTransfer is DirectoryTransfer)
                    {
                        if (baseTransfer.Source is DirectoryLocation)
                        {
                            this.DirectoryPath = (baseTransfer.Source as DirectoryLocation).DirectoryPath;
                        }
                        else if (baseTransfer.Destination is DirectoryLocation)
                        {
                            this.DirectoryPath = (baseTransfer.Destination as DirectoryLocation).DirectoryPath;
                        }
                    }

                    this.stream.Position = this.baseTransfer.StreamJournalOffset + this.baseTransferSize;

#if BINARY_SERIALIZATION
                    this.baseTransfer.ProgressTracker.AddProgress((TransferProgressTracker)this.formatter.Deserialize(this.stream));
#else
                    this.baseTransfer.ProgressTracker.AddProgress((TransferProgressTracker)this.ReadObject(this.progressCheckerSerializer));
#endif
                    this.baseTransfer.ProgressTracker.Journal = this;
                    this.baseTransfer.ProgressTracker.StreamJournalOffset = this.baseTransfer.StreamJournalOffset + this.baseTransferSize;
                    return this.baseTransfer;
                }
            }
        }

        internal void AddTransfer(Transfer transfer)
        {
            if (null != this.baseTransfer)
            {
                throw new InvalidOperationException(Resources.OnlyOneTransferAllowed);
            }

            lock (this.journalLock)
            {
                if (transfer is DirectoryTransfer)
                {
                    if (transfer.Source is DirectoryLocation)
                    {
                        this.DirectoryPath = (transfer.Source as DirectoryLocation).DirectoryPath;
                    }
                    else if (transfer.Destination is DirectoryLocation)
                    {
                        this.DirectoryPath = (transfer.Destination as DirectoryLocation).DirectoryPath;
                    }
                }

                transfer.Journal = this;
                transfer.StreamJournalOffset = contentOffset + sizeof(int);
                this.stream.Position = transfer.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer);
                this.baseTransferSize = (int)this.stream.Position;

                this.stream.Position = contentOffset;
                this.stream.Write(BitConverter.GetBytes(this.baseTransferSize), 0, sizeof(int));

#else               
                transfer.IsStreamJournal = true;
                this.WriteObject(this.transferSerializer, transfer);
#endif

                transfer.ProgressTracker.Journal = this;
                transfer.ProgressTracker.StreamJournalOffset = transfer.StreamJournalOffset + baseTransferSize;

                this.stream.Position = transfer.ProgressTracker.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer.ProgressTracker);
#else
                this.WriteObject(this.progressCheckerSerializer, transfer.ProgressTracker);
#endif
                this.baseTransfer = transfer;
                this.stream.Flush();
            }
        }

        internal void AddSubtransfer(SingleObjectTransfer transfer)
        {
            lock (this.journalLock)
            {
                long offset = this.SearchFreeOffset();
                transfer.Journal = this;
                transfer.StreamJournalOffset = offset + 2 * sizeof(long);

                transfer.ProgressTracker.Journal = this;
                transfer.ProgressTracker.StreamJournalOffset = transfer.StreamJournalOffset + TransferItemContentSize;

                this.stream.Position = transfer.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer);
#else
                transfer.IsStreamJournal = true;
                this.WriteObject(this.transferSerializer, transfer);
#endif

                this.stream.Position = transfer.ProgressTracker.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer.ProgressTracker);
#else
                this.WriteObject(this.progressCheckerSerializer, transfer.ProgressTracker);
#endif

                if (0 == this.usedChunkHead)
                {
                    this.usedChunkHead = offset;
                    this.usedChunkTail = this.usedChunkHead;
                    
                    // Set the transferEntry's previous and next trunk to 0.
                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                }
                else
                {
                    // Set current tail's next to the transferEntry's offset.
                    this.stream.Position = this.usedChunkTail + sizeof(long);
                    this.stream.Write(BitConverter.GetBytes(offset), 0, sizeof(long));

                    // Set the transferEntry's previous trunk to current tail.
                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(this.usedChunkTail), 0, sizeof(long));

                    // Set the transferEntry's next trunk to 0.
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));

                    this.usedChunkTail = offset;
                }

                this.WriteJournalHead();
                this.stream.Flush();
            }
        }

        internal void RemoveTransfer(Transfer transfer)
        {
            lock (this.journalLock)
            {
                if (transfer.StreamJournalOffset == this.baseTransfer.StreamJournalOffset)
                {
                    this.stream.SetLength(0);
                    return;
                }

                // Mark this entry chunk to be free...
                long chunkOffset = transfer.StreamJournalOffset - 2 * sizeof(long);
                this.stream.Position = chunkOffset;

                long previousUsedChunk = this.ReadLong();
                long nextUsedChunk = this.ReadLong();

                // This chunk is free now, set its next free chunk to be 0.
                this.stream.Position = chunkOffset;

                if (0 == this.freeChunkHead)
                {
                    this.freeChunkHead = chunkOffset;
                    this.freeChunkTail = chunkOffset;

                    this.stream.Position = chunkOffset;
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                }
                else
                {
                    this.stream.Position = this.freeChunkTail;
                    this.stream.Write(BitConverter.GetBytes(chunkOffset), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    this.freeChunkTail = chunkOffset;
                }

                if (0 != previousUsedChunk)
                {
                    this.stream.Position = previousUsedChunk + sizeof(long);
                    this.stream.Write(BitConverter.GetBytes(nextUsedChunk), 0, sizeof(long));
                }
                else
                {
                    if (this.usedChunkHead != chunkOffset)
                    {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                    }

                    this.usedChunkHead = nextUsedChunk;
                }

                if (0 != nextUsedChunk)
                {
                    this.stream.Position = nextUsedChunk;
                    this.stream.Write(BitConverter.GetBytes(previousUsedChunk), 0, sizeof(long));
                }
                else
                {
                    if (this.usedChunkTail != chunkOffset)
                    {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                    }

                    this.usedChunkTail = previousUsedChunk;
                }

                this.WriteJournalHead();
                this.stream.Flush();
            }
        }

        internal void UpdateJournalItem(JournalItem item)
        {
            lock (this.journalLock)
            {
                this.stream.Position = item.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, item);
#else
                var transfer = item as Transfer;

                if (null != transfer)
                {
                    this.WriteObject(this.transferSerializer, transfer);
                }
                else
                {
                    this.WriteObject(this.progressCheckerSerializer, item as TransferProgressTracker);
                }
#endif
                this.stream.Flush();
            }
        }

        public IEnumerable<SingleObjectTransfer> ListSubTransfers()
        {
            long currentOffset = this.usedChunkHead;
            bool shouldBreak = false;

            while (true)
            {
                SingleObjectTransfer transfer = null;
                lock (this.journalLock)
                {
                    if (0 == this.usedChunkHead)
                    {
                        shouldBreak = true;
                    }
                    else
                    {
                        this.stream.Position = currentOffset;
                        
                        long previousUsedChunk = this.ReadLong();
                        long nextUsedChunk = this.ReadLong();

                        if (0 == previousUsedChunk)
                        {
                            if (this.usedChunkHead != currentOffset)
                            {
#if !NO_FILEFORMAT_EX
                                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                            }
                        }
                        else
                        {
                            if (this.usedChunkHead == currentOffset)
                            {
#if !NO_FILEFORMAT_EX
                                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                            }
                        }

                        try
                        {
#if BINARY_SERIALIZATION
                            transfer = this.formatter.Deserialize(this.stream) as SingleObjectTransfer;
#else                            
                            transfer = this.ReadObject(this.transferSerializer) as SingleObjectTransfer;
#endif
                        }
                        catch (Exception)
                        {
#if !NO_FILEFORMAT_EX
                            throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                            throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                        }

                        if (null == transfer)
                        {
#if !NO_FILEFORMAT_EX
                            throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                            throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                        }

                        transfer.StreamJournalOffset = currentOffset + 2 * sizeof(long);
                        transfer.Journal = this;

                        this.stream.Position = transfer.StreamJournalOffset + TransferItemContentSize;

                        TransferProgressTracker progressTracker = null;

                        try
                        {
#if BINARY_SERIALIZATION
                            progressTracker = this.formatter.Deserialize(this.stream) as TransferProgressTracker;
#else
                            progressTracker = this.ReadObject(this.progressCheckerSerializer) as TransferProgressTracker;
#endif
                        }
                        catch (Exception)
                        {
#if !NO_FILEFORMAT_EX
                            throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                            throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                        }

                        if (null == progressTracker)
                        {
#if !NO_FILEFORMAT_EX
                            throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                            throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                        }

                        transfer.ProgressTracker.AddProgress(progressTracker);
                        transfer.ProgressTracker.StreamJournalOffset = transfer.StreamJournalOffset + TransferItemContentSize;
                        transfer.ProgressTracker.Journal = this;

                        if (0 == nextUsedChunk)
                        {
                            if (this.usedChunkTail != currentOffset)
                            {
#if !NO_FILEFORMAT_EX
                                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                            }

                            shouldBreak = true;
                        }
                        else
                        {
                            if (this.usedChunkTail == currentOffset)
                            {
#if !NO_FILEFORMAT_EX
                                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                            }
                        }

                        currentOffset = nextUsedChunk;
                    }
                }

                if (null != transfer)
                {
                    yield return transfer;
                }

                if (shouldBreak)
                {
                    yield break;
                }
            }
        }

        private void WriteJournalHead()
        {
            this.stream.Position = JournalHeadOffset;
            this.stream.Write(BitConverter.GetBytes(this.usedChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.usedChunkTail), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.freeChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.freeChunkTail), 0, sizeof(long));
        }

        private long ReadLong()
        {
            this.ReadAndCheck(sizeof(long));
            return BitConverter.ToInt64(this.memoryBuffer, 0);
        }

        private int ReadInt()
        {
            this.ReadAndCheck(sizeof(int));
            return BitConverter.ToInt32(this.memoryBuffer, 0);
        }

        /// <summary>
        /// Read from journal file and check whether the read succeeded.
        /// </summary>
        /// <param name="length">Count of bytes need to read.</param>
        private void ReadAndCheck(int length)
        {
            this.AllocateBuffer(length);

            if (stream.Read(this.memoryBuffer, 0, length) < length)
            {
#if !NO_FILEFORMAT_EX
                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
            }
        }

        /// <summary>
        /// Allocate buffer from memory. This function will allocate buffer on granularity of BufferSizeGranularity.
        /// </summary>
        /// <param name="length"> Length of needed buffer.</param>
        private void AllocateBuffer(int length)
        {
            if ((null == this.memoryBuffer)
                || (this.memoryBuffer.Length < length))
            {
                int allocateLength = length;
                if (0 != length % BufferSizeGranularity)
                {
                    allocateLength = ((length / BufferSizeGranularity) + 1) * BufferSizeGranularity;
                }

                this.memoryBuffer = new byte[allocateLength];
            }
        }

        private long SearchFreeOffset()
        {
            if (0 != this.freeChunkHead)
            {
                long currentFreeChunk = this.freeChunkHead;

                if (this.freeChunkHead == this.freeChunkTail)
                {
                    this.freeChunkHead = 0;
                    this.freeChunkTail = 0;
                }
                else
                {
                    this.stream.Position = this.freeChunkHead;
                    this.ReadAndCheck(sizeof(long));
                    this.freeChunkHead = BitConverter.ToInt64(this.memoryBuffer, 0);

                    if (0 == this.freeChunkHead)
                    {
                        this.freeChunkTail = 0;
                    }
                }

                return currentFreeChunk;
            }
            else
            {
                return this.stream.Length <= ContentOffset ? ContentOffset : ((this.stream.Length - ContentOffset) / TransferChunkSize + 1) * TransferChunkSize + ContentOffset;
            }
        }

#if !BINARY_SERIALIZATION
        private void WriteObject(DataContractSerializer serializer, object instance)
        {
            this.serializerStream.SetLength(0);
            serializer.Serialize(this.serializerStream, instance);
            this.stream.Write(BitConverter.GetBytes(this.serializerStream.Length), 0, sizeof(long));
            this.stream.Write(this.serializerBuffer, 0, (int)this.serializerStream.Length);
        }

        private object ReadObject(DataContractSerializer serializer)
        {
            long serializerLength = this.ReadLong();
            this.serializerStream.SetLength(serializerLength);

            if (this.stream.Read(this.serializerBuffer, 0, (int)serializerLength) < (int)serializerLength)
            {
#if !NO_FILEFORMAT_EX
                throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
            }
            
            this.serializerStream.Position = 0;
            return serializer.Deserialize(this.serializerStream);
        }
#endif
    }
}
