//------------------------------------------------------------------------------
// <copyright file="StreamJournal.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
#if BINARY_SERIALIZATION
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading;
    using System.Threading.Tasks;
#endif

    internal class StreamJournal
    {
        //------------------------------------------------------------------------------------------------------
        // 0-255: Journal format version string: Assembly name + version
        // 256 - 511: Journal head, keep the list of used chunks for transfer instances and free chunks, and the count of sub-transfers preserved in the journal.
        // 512 - (SubTransferContentOffset-1) : Last 1K for progress tracker of the transfer instance, and rests are base transfer instance.
        // A base transfer can be a SingleObjectTransfer or a MultipleObjectTransfer, if it's a MultipleObjectTransfer,
        // there could be multiple subtransfers, each subtransfer is a SingleObjectTransfer.
        // SubTransferContentOffset- : Chunks for transfer instances
        // Size of each chunk is 10K, 9K for transfer instance, 1K for progress tracker of the transfer instance.
        // In the journal, it only allows one base transfer, which means user can only add one transfer to the checkpoint using stream journal.
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
        /// For each transfer instance, it saves the transfer object itself and the transfer's ProgressTracker in the journal.
        /// This is size to allocated for the transfer's ProgressTracker.
        /// </summary>
        private const int ProcessTrackerSize = 1024;

        /// <summary>
        /// For a subdirectory pending on listing, only write its relative path into journal.
        /// An Azure File name path can be no longer than 2048 characters.
        /// It occupies 40 bytes on .Net and 93 bytes on .Net Core to write a string with 16 characters.
        /// Here set the size to the maximum Azure File path length plus a fixed reserved length to make sure it won't mess up two neighboring strings.
        /// </summary>
        private const int SubDirectoryTransferChunkSize = 2048 + 128;

        /// <summary>
        /// It keeps a list of used transfer chunks and a list free transfers in the journal stream,
        /// journal head keeps the heads and tails for these two lists.
        /// </summary>
        private const int JournalHeadOffset = 256;

        /// <summary>
        /// Offset in stream for the beginning to persistant transfer instance.
        /// </summary>
        private const int ContentOffset = 512;

        /// <summary>
        /// Offset in stream for the beginning to sub-transfer instance.
        /// </summary>
        private const int SubTransferContentOffset = 40 * 1024;

        /// <summary>
        /// Size for directory transfer instance in the journal stream.
        /// </summary>
        private const int MaxTransferChunkSize = 40 * 1024;

        /// <summary>
        /// In the journal, it only allows one base transfer, which means user can only add one transfer to the checkpoint using stream journal.
        /// A base transfer can be a SingleObjectTransfer or a MultipleObjectTransfer, if it's a MultipleObjectTransfer,
        /// there could be multiple subtransfers, each subtransfer is a SingleObjectTransfer.
        /// </summary>
        private Transfer baseTransfer = null;

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
        private DataContractSerializer subDirTransferSerializer = new DataContractSerializer(typeof(SubDirectoryTransfer));
        private DataContractSerializer progressCheckerSerializer = new DataContractSerializer(typeof(TransferProgressTracker));
        private DataContractSerializer continuationTokenSerializer = new DataContractSerializer(typeof(SerializableListContinuationToken));
#endif

        // Three lists: single transfer list, ongoing subdirectory transfer list, subdirectory transfer list.
        long singleTransferChunkHead = 0;
        long singleTransferChunkTail = 0;
        long OngoingSubDirTransferChunkHead = 0;
        long OngoingSubDirTransferChunkTail = 0;
        long subDirTransferChunkHead = 0;
        long subDirTransferChunkTail = 0;

        long subDirTransferNextWriteOffset = 0;
        long subDirTransferCurrentReadOffset = 0;

        long freeChunkHead = 0;
        long freeChunkTail = 0;

        long preservedChunkCount = 0;

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
            serializerBuffer = new byte[MaxTransferChunkSize];
            serializerStream = new MemoryStream(serializerBuffer);
#endif
        }

        public Transfer Initialize()
        {
            lock (this.journalLock)
            {
                if (this.ReadAndCheckEmpty())
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
                    this.stream.Position = 0;
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
                    this.singleTransferChunkHead = this.ReadLong();
                    this.singleTransferChunkTail = this.ReadLong();
                    this.OngoingSubDirTransferChunkHead = this.ReadLong();
                    this.OngoingSubDirTransferChunkTail = this.ReadLong();
                    this.subDirTransferChunkHead = this.ReadLong();
                    this.subDirTransferChunkTail = this.ReadLong();
                    this.freeChunkHead = this.ReadLong();
                    this.freeChunkTail = this.ReadLong();
                    this.subDirTransferNextWriteOffset = this.ReadLong();
                    this.subDirTransferCurrentReadOffset = this.ReadLong();
                    this.preservedChunkCount = this.ReadLong();

                    // absolute path
                    this.stream.Position = ContentOffset;

#if BINARY_SERIALIZATION
                    this.baseTransfer = (Transfer)this.formatter.Deserialize(this.stream);
#else
                    this.baseTransfer = this.ReadObject(this.transferSerializer) as Transfer;
#endif
                    this.baseTransfer.StreamJournalOffset = ContentOffset;
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

                    this.stream.Position = SubTransferContentOffset - ProcessTrackerSize;

#if BINARY_SERIALIZATION
                    this.baseTransfer.ProgressTracker.AddProgress((TransferProgressTracker)this.formatter.Deserialize(this.stream));
#else
                    this.baseTransfer.ProgressTracker.AddProgress((TransferProgressTracker)this.ReadObject(this.progressCheckerSerializer));
#endif
                    this.baseTransfer.ProgressTracker.Journal = this;
                    this.baseTransfer.ProgressTracker.StreamJournalOffset = SubTransferContentOffset - ProcessTrackerSize;
                    return this.baseTransfer;
                }
            }
        }

        internal void AddOngoingSubDirTransfer(SubDirectoryTransfer directoryTransfer)
        {
            lock (this.journalLock)
            {
                long offset = this.SearchFreeOffset();

                directoryTransfer.Journal = this;
                directoryTransfer.StreamJournalOffset = offset + 2 * sizeof(long);

                this.stream.Position = offset + 2 * sizeof(long);
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, directoryTransfer);
#else
                this.WriteObject(this.subDirTransferSerializer, directoryTransfer);
#endif

                long continuationTokenOffset = offset + 2 * sizeof(long) + 4096;
                this.stream.Position = continuationTokenOffset;
                directoryTransfer.ListContinuationToken.Journal = this;
                directoryTransfer.ListContinuationToken.StreamJournalOffset = continuationTokenOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, directoryTransfer.ListContinuationToken);
#else
                this.WriteObject(this.continuationTokenSerializer, directoryTransfer.ListContinuationToken);
#endif

                if (this.OngoingSubDirTransferChunkHead == 0)
                {
                    if (this.OngoingSubDirTransferChunkTail != 0)
                    {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                    }

                    this.OngoingSubDirTransferChunkHead = offset;
                    this.OngoingSubDirTransferChunkTail = offset;

                    this.stream.Position = this.OngoingSubDirTransferChunkHead;
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                }
                else
                {
                    this.stream.Position = this.OngoingSubDirTransferChunkTail + sizeof(long);
                    this.stream.Write(BitConverter.GetBytes(offset), 0, sizeof(long));

                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(this.OngoingSubDirTransferChunkTail), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));

                    this.OngoingSubDirTransferChunkTail = offset;
                }

                this.WriteJournalHead();
                this.stream.Flush();
            }
        }

        internal void RemoveSubDirTransfer(SubDirectoryTransfer directoryTransfer)
        {
            lock (this.journalLock)
            {
                long offset = directoryTransfer.StreamJournalOffset - 2 * sizeof(long);
                this.FreeChunk(offset, ref this.OngoingSubDirTransferChunkHead, ref this.OngoingSubDirTransferChunkTail);
                this.stream.Flush();
            }
        }

        internal void RemoveFirstSubDirTransfer()
        {
            lock (this.journalLock)
            {
                if (0 == this.subDirTransferCurrentReadOffset
                    || 0 == this.subDirTransferChunkHead
                    || 0 == this.subDirTransferChunkTail
                    || this.subDirTransferChunkTail > this.subDirTransferNextWriteOffset)
                {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                    throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                }

                this.stream.Position = this.subDirTransferCurrentReadOffset;
                Array.Clear(memoryBuffer, 0, SubDirectoryTransferChunkSize);
                this.stream.Write(memoryBuffer, 0, SubDirectoryTransferChunkSize);

                this.subDirTransferCurrentReadOffset += SubDirectoryTransferChunkSize;

                if ((this.subDirTransferCurrentReadOffset + SubDirectoryTransferChunkSize) > (this.subDirTransferChunkHead + TransferChunkSize))
                {
                    this.stream.Position = this.subDirTransferChunkHead + sizeof(long);
                    long nextChunk = this.ReadLong();

                    if (0 == this.freeChunkHead)
                    {
                        this.freeChunkHead = this.subDirTransferChunkHead;
                        this.freeChunkTail = this.subDirTransferChunkHead;

                        this.stream.Position = this.subDirTransferChunkHead;
                        this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                        this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    }
                    else
                    {
                        this.stream.Position = this.freeChunkTail;
                        this.stream.Write(BitConverter.GetBytes(this.subDirTransferChunkHead), 0, sizeof(long));
                        this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                        this.freeChunkTail = this.subDirTransferChunkHead;
                    }

                    if (0 == nextChunk && this.subDirTransferChunkHead != this.subDirTransferChunkTail)
                    {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                    }

                    if (this.subDirTransferChunkHead == this.subDirTransferChunkTail)
                    {
                        if (0 != nextChunk)
                        {
#if !NO_FILEFORMAT_EX
                            throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                            throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                        }

                        this.subDirTransferChunkHead = 0;
                        this.subDirTransferChunkTail = 0;
                        this.subDirTransferCurrentReadOffset = 0;
                        this.subDirTransferNextWriteOffset = 0;
                    }
                    else
                    {
                        this.subDirTransferChunkHead = nextChunk;
                        this.subDirTransferCurrentReadOffset = nextChunk + 2 * sizeof(long);
                    }
                }

                this.WriteJournalHead();
                this.stream.Flush();
            }
        }

        internal string PeekSubDirTransfer()
        {
            if (0 == this.subDirTransferCurrentReadOffset)
            {
                return null;
            }
            else
            {
                lock (this.journalLock)
                {
                    if (this.subDirTransferCurrentReadOffset == this.subDirTransferNextWriteOffset)
                    {
                        return null;
                    }


                    this.stream.Position = this.subDirTransferCurrentReadOffset;
#if BINARY_SERIALIZATION
                    return this.formatter.Deserialize(this.stream) as string;
#else
                    return (string)this.ReadObject(this.stringSerializer);
#endif
                }
            }
        }

        internal void AddSubDirTransfer(string relativePath)
        {
            lock (this.journalLock)
            {
                long writingOffset = 0;
                if (0 == this.subDirTransferNextWriteOffset)
                {
                    if (this.subDirTransferChunkHead != 0 || this.subDirTransferChunkTail != 0)
                    {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                    }
                    else
                    {
                        long offset = this.SearchFreeOffset();
                        this.subDirTransferChunkHead = offset;
                        this.subDirTransferChunkTail = offset;
                        this.stream.Position = offset;
                        this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                        this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                        writingOffset = offset + 2 * sizeof(long);
                        // The first sub directory transfer added.
                        this.subDirTransferCurrentReadOffset = writingOffset;
                    }

                    this.subDirTransferNextWriteOffset = writingOffset;
                }
                else if (this.subDirTransferChunkHead == 0 || this.subDirTransferChunkTail == 0 || this.subDirTransferChunkTail >= this.subDirTransferNextWriteOffset)
                {
#if !NO_FILEFORMAT_EX
                        throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                }

                this.stream.Position = this.subDirTransferNextWriteOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, relativePath);
#else
                this.WriteObject(this.stringSerializer, relativePath);
#endif

                this.subDirTransferNextWriteOffset += SubDirectoryTransferChunkSize;

                if (this.subDirTransferChunkTail + TransferChunkSize < this.subDirTransferNextWriteOffset + SubDirectoryTransferChunkSize)
                {
                    // Current chunk is full, allocate a new one
                    long offset = this.SearchFreeOffset();
                    this.stream.Position = this.subDirTransferChunkTail + sizeof(long);
                    this.stream.Write(BitConverter.GetBytes(offset), 0, sizeof(long));
                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(this.subDirTransferChunkTail), 0, sizeof(long));
                    this.subDirTransferChunkTail = offset;

                    this.subDirTransferNextWriteOffset = offset + 2 * sizeof(long);
                }

                this.WriteJournalHead();
                this.stream.Flush();
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
                transfer.StreamJournalOffset = ContentOffset;
                this.stream.Position = transfer.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer);
#else
                transfer.IsStreamJournal = true;
                this.WriteObject(this.transferSerializer, transfer);
#endif
                transfer.ProgressTracker.Journal = this;
                transfer.ProgressTracker.StreamJournalOffset = SubTransferContentOffset - ProcessTrackerSize;

                this.stream.Position = transfer.ProgressTracker.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer.ProgressTracker);
#else
                this.WriteObject(this.progressCheckerSerializer, transfer.ProgressTracker);
#endif
                this.baseTransfer = transfer;
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
                if (transfer.Source is FileLocation)
                {
                    (transfer.Source as FileLocation).IsStreamJournal = true;
                }
                else if (transfer.Destination is FileLocation)
                {
                    (transfer.Destination as FileLocation).IsStreamJournal = true;
                }
                this.WriteObject(this.transferSerializer, transfer);
#endif

                this.stream.Position = transfer.ProgressTracker.StreamJournalOffset;
#if BINARY_SERIALIZATION
                this.formatter.Serialize(this.stream, transfer.ProgressTracker);
#else
                this.WriteObject(this.progressCheckerSerializer, transfer.ProgressTracker);
#endif

                if (0 == this.singleTransferChunkHead)
                {
                    this.singleTransferChunkHead = offset;
                    this.singleTransferChunkTail = this.singleTransferChunkHead;

                    // Set the transferEntry's previous and next trunk to 0.
                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));
                }
                else
                {
                    // Set current tail's next to the transferEntry's offset.
                    this.stream.Position = this.singleTransferChunkTail + sizeof(long);
                    this.stream.Write(BitConverter.GetBytes(offset), 0, sizeof(long));

                    // Set the transferEntry's previous trunk to current tail.
                    this.stream.Position = offset;
                    this.stream.Write(BitConverter.GetBytes(this.singleTransferChunkTail), 0, sizeof(long));

                    // Set the transferEntry's next trunk to 0.
                    this.stream.Write(BitConverter.GetBytes(0L), 0, sizeof(long));

                    this.singleTransferChunkTail = offset;
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
                    try
                    {
                        this.stream.SetLength(0);
                    }
                    catch (NotSupportedException)
                    {
                        // Catch by design, if user provided stream doesn't support SetLength, 
                        // user should be responsible to remove journal after transfer.
                        // TODO: Logging
                    }
                    
                    return;
                }

                // Mark this entry chunk to be free...
                long chunkOffset = transfer.StreamJournalOffset - 2 * sizeof(long);

                this.FreeChunk(chunkOffset, ref this.singleTransferChunkHead, ref this.singleTransferChunkTail);
                this.stream.Flush();
            }
        }

        internal void FreeChunk(long chunkOffset, ref long usedChunkHead, ref long usedChunkTail)
        {
            // Mark this entry chunk to be free...
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
                if (usedChunkHead != chunkOffset)
                {
#if !NO_FILEFORMAT_EX
                    throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                }

                usedChunkHead = nextUsedChunk;
            }

            if (0 != nextUsedChunk)
            {
                this.stream.Position = nextUsedChunk;
                this.stream.Write(BitConverter.GetBytes(previousUsedChunk), 0, sizeof(long));
            }
            else
            {
                if (usedChunkTail != chunkOffset)
                {
#if !NO_FILEFORMAT_EX
                    throw new FileFormatException(Resources.RestartableLogCorrupted);
#else
                        throw new InvalidOperationException(Resources.RestartableLogCorrupted);
#endif
                }

                usedChunkTail = previousUsedChunk;
            }

            this.WriteJournalHead();
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
                    return;
                }

                var progressChecker = item as TransferProgressTracker;
                if (null != progressChecker)
                {
                    this.WriteObject(this.progressCheckerSerializer, progressChecker);
                    return;
                }

                var serializableContinuationToken = item as SerializableListContinuationToken;
                if (null != serializableContinuationToken)
                {
                    this.WriteObject(this.continuationTokenSerializer, serializableContinuationToken);
                    return;
                }
#endif
            }
        }

        public IEnumerable<SubDirectoryTransfer> ListSubDirTransfers()
        {
            long currentOffset = this.OngoingSubDirTransferChunkHead;
            bool shouldBreak = false;

            while (true)
            {
                SubDirectoryTransfer transfer = null;
                lock (this.journalLock)
                {
                    if (0 == this.OngoingSubDirTransferChunkHead)
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
                            if (this.OngoingSubDirTransferChunkHead != currentOffset)
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
                            if (this.OngoingSubDirTransferChunkHead == currentOffset)
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
                            transfer = this.formatter.Deserialize(this.stream) as SubDirectoryTransfer;
#else
                            transfer = this.ReadObject(this.subDirTransferSerializer) as SubDirectoryTransfer;
#endif
                        }
                        catch
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

                        this.stream.Position = transfer.StreamJournalOffset + 4096;
#if BINARY_SERIALIZATION
                        transfer.ListContinuationToken = this.formatter.Deserialize(this.stream) as SerializableListContinuationToken;
#else
                        transfer.ListContinuationToken = this.ReadObject(this.continuationTokenSerializer) as SerializableListContinuationToken;
#endif
                        transfer.ListContinuationToken.Journal = this;
                        transfer.ListContinuationToken.StreamJournalOffset = transfer.StreamJournalOffset + 4096;

                        if (0 == nextUsedChunk)
                        {
                            if (this.OngoingSubDirTransferChunkTail != currentOffset)
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
                            if (this.OngoingSubDirTransferChunkTail == currentOffset)
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

        public IEnumerable<SingleObjectTransfer> ListSubTransfers()
        {
            long currentOffset = this.singleTransferChunkHead;
            bool shouldBreak = false;

            while (true)
            {
                SingleObjectTransfer transfer = null;
                lock (this.journalLock)
                {
                    if (0 == this.singleTransferChunkHead)
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
                            if (this.singleTransferChunkHead != currentOffset)
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
                            if (this.singleTransferChunkHead == currentOffset)
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

                            if (transfer.Source is FileLocation)
                            {
                                (transfer.Source as FileLocation).SetDirectoryPath(this.DirectoryPath);
                            }
                            else if (transfer.Destination is FileLocation)
                            {
                                (transfer.Destination as FileLocation).SetDirectoryPath(this.DirectoryPath);
                            }
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
                            if (this.singleTransferChunkTail != currentOffset)
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
                            if (this.singleTransferChunkTail == currentOffset)
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
            this.stream.Write(BitConverter.GetBytes(this.singleTransferChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.singleTransferChunkTail), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.OngoingSubDirTransferChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.OngoingSubDirTransferChunkTail), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.subDirTransferChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.subDirTransferChunkTail), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.freeChunkHead), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.freeChunkTail), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.subDirTransferNextWriteOffset), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.subDirTransferCurrentReadOffset), 0, sizeof(long));
            this.stream.Write(BitConverter.GetBytes(this.preservedChunkCount), 0, sizeof(long));
        }

        private long ReadLong()
        {
            this.ReadAndCheck(sizeof(long));
            return BitConverter.ToInt64(this.memoryBuffer, 0);
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
        /// Read from journal file and check whether the journal file is empty.
        /// </summary>
        /// <returns>True if the journal file is empty.</returns>
        private bool ReadAndCheckEmpty()
        {
            this.stream.Position = 0;

            this.AllocateBuffer(JournalHeadOffset);
            
            if (this.stream.Read(this.memoryBuffer, 0, JournalHeadOffset) < JournalHeadOffset)
            {
                return true;
            }

            return !this.memoryBuffer.Any(c => c != 0);
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
                long currentFreeChunkOffset = this.stream.Length <= SubTransferContentOffset ? SubTransferContentOffset : (this.preservedChunkCount * TransferChunkSize + SubTransferContentOffset);
                this.preservedChunkCount++;
                return currentFreeChunkOffset;
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
