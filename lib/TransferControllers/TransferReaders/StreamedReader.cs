//------------------------------------------------------------------------------
// <copyright file="StreamedReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Security;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class StreamedReader : TransferReaderWriterBase
    {
        /// <summary>
        /// Source stream to be read from. 
        /// It's a user input stream or a FileStream with the user input FilePath in source location.
        /// </summary>
        private Stream inputStream;

        /// <summary>
        /// Value to indicate whether the input stream is a file stream owned by this reader or input by user.
        /// If it's a file stream owned by this reader, we should close it when reading is finished.
        /// </summary>
        private bool ownsStream;

        /// <summary>
        /// Transfer job instance.
        /// </summary>
        private TransferJob transferJob;

        /// <summary>
        /// Transfer window in check point.
        /// </summary>
        private Queue<long> lastTransferWindow;

        private volatile State state;

        /// <summary>
        /// Work token indicates whether this reader has work, could be 0(no work) or 1(has work).
        /// </summary>
        private volatile int workToken;

        private long readLength = 0;

        private long readCompleted = 0;

        private int setCompletionDone = 0;

        /// <summary>
        /// Stream to read from source and calculate md5 hash of source.
        /// </summary>
        private MD5HashStream md5HashStream;

        private string filePath = null;

        public StreamedReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.transferJob = this.SharedTransferData.TransferJob;
            this.workToken = 1;
            this.readLength = 0;
        }

        private enum State
        {
            OpenInputStream,
            ReadStream,
            Error,
            Finished
        }

        public override bool IsFinished
        {
            get
            {
                return this.state == State.Error || this.state == State.Finished;
            }
        }

        public override bool HasWork
        {
            get
            {
                return this.workToken == 1;
            }
        }

        public override async Task DoWorkInternalAsync()
        {
            switch (this.state)
            {
                case State.OpenInputStream:
                    await this.OpenInputStreamAsync();
                    break;
                case State.ReadStream:
                    await this.ReadStreamAsync();
                    break;
                case State.Error:
                case State.Finished:
                    break;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                this.CloseOwnStream();

                if (null != this.md5HashStream)
                {
                    this.md5HashStream.Dispose();
                    this.md5HashStream = null;
                }
            }
        }

        private async Task OpenInputStreamAsync()
        {
            Debug.Assert(
                State.OpenInputStream == this.state,
                "OpenInputStreamAsync called, but state is not OpenInputStream.");

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            await Task.Yield();

            this.NotifyStarting();
            this.Controller.CheckCancellation();

            if (this.transferJob.Source.Type == TransferLocationType.Stream)
            {
                StreamLocation streamLocation = this.transferJob.Source as StreamLocation;
                this.inputStream = streamLocation.Stream;
                this.ownsStream = false;

                if (!this.inputStream.CanRead)
                {
                    throw new NotSupportedException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.StreamMustSupportReadException,
                        "inputStream"));
                }
            }
            else
            {
                FileLocation fileLocation = this.transferJob.Source as FileLocation;
                Debug.Assert(
                    null != fileLocation,
                    "Initializing StreamedReader instance, but source is neither a stream nor a file");

                try
                {
                    if (fileLocation.RelativePath != null
                        && fileLocation.RelativePath.Length > Constants.MaxRelativePathLength)
                    {
                        string errorMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.RelativePathTooLong,
                            fileLocation.RelativePath);
                        throw new TransferException(TransferErrorCode.OpenFileFailed, errorMessage);
                    }

                    this.filePath = fileLocation.FilePath;
#if DOTNET5_4
                    if (Interop.CrossPlatformHelpers.IsWindows)
                    {
                        filePath = LongPath.ToUncPath(fileLocation.FilePath);
                    }
                    // Attempt to open the file first so that we throw an exception before getting into the async work
                    this.inputStream = new FileStream(
                        filePath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.Read);
#else
                    this.inputStream = LongPathFile.Open(
                            this.filePath,
                            FileMode.Open,
                            FileAccess.Read,
                            FileShare.Read);
#endif
                    this.ownsStream = true;
                }
                catch (Exception ex)
                {
                    if ((ex is NotSupportedException) ||
                        (ex is IOException) ||
                        (ex is UnauthorizedAccessException) ||
                        (ex is SecurityException) ||
                        (ex is ArgumentException && !(ex is ArgumentNullException)))
                    {
                        string exceptionMessage = string.Format(
                                    CultureInfo.CurrentCulture,
                                    Resources.FailedToOpenFileException,
                                    fileLocation.FilePath,
                                    ex.Message);

                        throw new TransferException(
                                TransferErrorCode.OpenFileFailed,
                                exceptionMessage,
                                ex);
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            try
            {
                this.SharedTransferData.TotalLength = this.inputStream.Length;
            }
            catch (NotSupportedException)
            {
                this.SharedTransferData.TotalLength = -1;
            }

            // Only do calculation related to transfer window when the file contains multiple chunks.
            if (!this.EnableOneChunkFileOptimization)
            {
                var checkpoint = this.transferJob.CheckPoint;

                checkpoint.TransferWindow.Sort();

                this.readLength = checkpoint.EntryTransferOffset;

                if (checkpoint.TransferWindow.Any())
                {
                    // The size of last block can be smaller than BlockSize.
                    this.readLength -= Math.Min(checkpoint.EntryTransferOffset - checkpoint.TransferWindow.Last(), this.SharedTransferData.BlockSize);
                    this.readLength -= (checkpoint.TransferWindow.Count - 1) * this.SharedTransferData.BlockSize;
                }

                if (this.readLength < 0)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }
                else if ((checkpoint.EntryTransferOffset > 0) && (!this.inputStream.CanSeek))
                {
                    throw new NotSupportedException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.StreamMustSupportSeekException,
                        "inputStream"));
                }

                this.lastTransferWindow = new Queue<long>(this.transferJob.CheckPoint.TransferWindow);
            }

            this.md5HashStream = new MD5HashStream(
                this.inputStream,
                this.transferJob.CheckPoint.EntryTransferOffset,
                true);

            this.PreProcessed = true;

            if (this.readLength != this.SharedTransferData.TotalLength)
            {
                // Change the state to 'ReadStream' before awaiting MD5 calculation task to not block the reader.
                this.state = State.ReadStream;
                this.workToken = 1;
            }
            else
            {
                Interlocked.Exchange(ref this.readCompleted, 1);
            }

            if (!this.md5HashStream.FinishedSeparateMd5Calculator)
            {
                await Task.Run(() =>
                {
                    this.md5HashStream.CalculateMd5(this.Scheduler.MemoryManager, this.Controller.CheckCancellation);
                });
            }

            this.SetChunkFinish();
        }

        private async Task ReadStreamAsync()
        {
            Debug.Assert(
                this.state == State.ReadStream || this.state == State.Error,
                "ReadChunks called, but state isn't ReadStream or Error");

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            // Work with yield is good for overall scheduling efficiency, i.e. other works would be scheduled faster.
            // While work without yield is good for current files reading efficiency(e.g. transfering only 1 1TB file).
            // So when file has only one chunk, the possibility of current file influencing overall throughput might be less.
            // And in this case do Yield, otherwise, temporarily work without yield.
            // TODO: the threshold to do yield could be further tuned.
            if (this.EnableOneChunkFileOptimization)
            { 
                await Task.Yield();
            }

            byte[][] memoryBuffer = this.Scheduler.MemoryManager.RequireBuffers(this.SharedTransferData.MemoryChunksRequiredEachTime);

            if (null != memoryBuffer)
            {
                long startOffset = 0;

                // Only do calculation related to transfer window when the file contains multiple chunks.
                if (!this.EnableOneChunkFileOptimization)
                {
                    if (0 != this.lastTransferWindow.Count)
                    {
                        startOffset = this.lastTransferWindow.Dequeue();
                    }
                    else
                    {
                        bool canRead = false;

                        // TransferWindow.Count is not necessary to be included in TransferWindowLock block, as current StreamReader
                        // is the only entry for adding TransferWindow size, and the logic adding TransferWindow size is always executed in one thread. 
                        if (this.transferJob.CheckPoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                        {
                            startOffset = this.transferJob.CheckPoint.EntryTransferOffset;

                            if ((this.SharedTransferData.TotalLength < 0) || (this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength))
                            {
                                lock (this.transferJob.CheckPoint.TransferWindowLock)
                                {
                                    if ((this.SharedTransferData.TotalLength < 0) || (this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength))
                                    {
                                        this.transferJob.CheckPoint.TransferWindow.Add(startOffset);
                                    }
                                }

                                this.transferJob.CheckPoint.EntryTransferOffset = Math.Min(
                                        this.transferJob.CheckPoint.EntryTransferOffset + this.SharedTransferData.BlockSize,
                                        this.SharedTransferData.TotalLength < 0 ? long.MaxValue : this.SharedTransferData.TotalLength);

                                canRead = true;
                            }
                        }

                        if (!canRead)
                        {
                            this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                            this.workToken = 1;
                            return;
                        }
                    }
                }

                if ((this.SharedTransferData.TotalLength > 0) && ((startOffset > this.SharedTransferData.TotalLength)
                    || (startOffset < 0)))
                {
                    this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                ReadDataState asyncState = new ReadDataState
                {
                    MemoryBuffer = memoryBuffer,
                    BytesRead = 0,
                    StartOffset = startOffset,
                    Length = (int)Math.Min(this.SharedTransferData.BlockSize, this.SharedTransferData.TotalLength > 0 ? (this.SharedTransferData.TotalLength - startOffset) : long.MaxValue),
                    MemoryManager = this.Scheduler.MemoryManager,
                };

                using (asyncState)
                {
                    await this.ReadChunkAsync(asyncState);
                }
            }

            this.SetHasWork();
        }

        private async Task ReadChunkAsync(ReadDataState asyncState)
        {
            Debug.Assert(null != asyncState, "asyncState object expected");
            Debug.Assert(
                this.state == State.ReadStream || this.state == State.Error,
                "ReadChunkAsync called, but state isn't Upload or Error");

            int readBytes = await this.md5HashStream.ReadAsync(
                asyncState.StartOffset + asyncState.BytesRead,
                asyncState.MemoryBuffer,
                asyncState.BytesRead,
                asyncState.Length - asyncState.BytesRead,
                this.CancellationToken);

            // If a parallel operation caused the controller to be placed in
            // error state exit early to avoid unnecessary I/O.
            // Note that this check needs to be after the EndRead operation
            // above to avoid leaking resources.
            if (this.state == State.Error)
            {
                return;
            }

            asyncState.BytesRead += readBytes;

            if (0 == readBytes)
            {
                this.ReadingDataHandler(asyncState, true);
                return;
            }

            if (asyncState.BytesRead < asyncState.Length)
            {
                await this.ReadChunkAsync(asyncState);
            }
            else
            {
                this.ReadingDataHandler(asyncState, false);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void ReadingDataHandler(ReadDataState asyncState, bool endofStream)
        {
            this.Controller.CheckCancellation();

            if (!this.md5HashStream.MD5HashTransformBlock(asyncState.StartOffset, asyncState.MemoryBuffer, 0, asyncState.BytesRead))
            {
                // Error info has been set in Calculate MD5 action, just return
                return;
            }

            TransferData transferData = new TransferData(this.Scheduler.MemoryManager)
            {
                StartOffset = asyncState.StartOffset,
                Length = asyncState.BytesRead,
                MemoryBuffer = asyncState.MemoryBuffer
            };

            long currentReadLength = Interlocked.Add(ref this.readLength, asyncState.BytesRead);

            if ((currentReadLength == this.SharedTransferData.TotalLength) || endofStream)
            {
                Interlocked.Exchange(ref this.readCompleted, 1);

                // Should only get into this block once.
                if (-1 == this.SharedTransferData.TotalLength)
                {
                    this.SharedTransferData.UpdateTotalLength(this.readLength);
                }
            }

            if (endofStream && (-1 != this.SharedTransferData.TotalLength) && (currentReadLength != this.SharedTransferData.TotalLength))
            {
                throw new TransferException(Resources.SourceChangedException);
            }

            asyncState.MemoryBuffer = null;
            this.SharedTransferData.AvailableData.TryAdd(transferData.StartOffset, transferData);

            this.SetChunkFinish();
        }

        private void SetHasWork()
        {
            if (this.HasWork)
            {
                return;
            }

            // Check if we have blocks available to download.
            if ((null != this.lastTransferWindow && this.lastTransferWindow.Any())
                || -1 == this.SharedTransferData.TotalLength
                || this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
            {
                this.workToken = 1;
                return;
            }
        }

        private void SetChunkFinish()
        {
            if (1 == Interlocked.Read(ref this.readCompleted))
            {
                if (0 == Interlocked.Exchange(ref this.setCompletionDone, 1))
                {
                    this.state = State.Finished;
                    if (!this.md5HashStream.SucceededSeparateMd5Calculator)
                    {
                        return;
                    }

                    var md5 = this.md5HashStream.MD5HashTransformFinalBlock();
                    this.CloseOwnStream();

                    Attributes attributes = new Attributes()
                    {
                        ContentMD5 = md5,
                        OverWriteAll = false
                    };

                    if (this.transferJob.Transfer.PreserveSMBAttributes)
                    {
                        if (!string.IsNullOrEmpty(this.filePath))
                        {
                            DateTimeOffset? creationTime;
                            DateTimeOffset? lastWriteTime;
                            FileAttributes? fileAttributes;

                            LongPathFile.GetFileProperties(this.filePath, out creationTime, out lastWriteTime, out fileAttributes);

                            attributes.CloudFileNtfsAttributes = Utils.LocalAttributesToAzureFileNtfsAttributes(fileAttributes.Value);
                            attributes.CreationTime = creationTime;
                            attributes.LastWriteTime = lastWriteTime;
                        }
                    }

                    this.SharedTransferData.Attributes = attributes;
                }
            }
        }

        private void CloseOwnStream()
        {
            if (this.ownsStream)
            {
                if (null != this.inputStream)
                {
#if DOTNET5_4
                    this.inputStream.Dispose();
#else
                    this.inputStream.Close();
#endif
                    this.inputStream = null;
                }
            }
        }
    }
}