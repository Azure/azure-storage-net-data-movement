//------------------------------------------------------------------------------
// <copyright file="StreamedReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
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
        /// Countdown event to track the download status.
        /// Its count should be the same with count of chunks to be read.
        /// </summary>
        private CountdownEvent countdownEvent;

        /// <summary>
        /// Transfer window in check point.
        /// </summary>
        private Queue<long> lastTransferWindow;

        private volatile State state;

        private volatile bool hasWork;

        /// <summary>
        /// Stream to read from source and calculate md5 hash of source.
        /// </summary>
        private MD5HashStream md5HashStream;

        public StreamedReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.transferJob = this.SharedTransferData.TransferJob;
            this.hasWork = true;
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
                return this.hasWork;
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

                if (null != this.countdownEvent)
                {
                    this.countdownEvent.Dispose();
                }
            }
        }

        private async Task OpenInputStreamAsync()
        {
            Debug.Assert(
                State.OpenInputStream == this.state,
                "OpenInputStreamAsync called, but state is not OpenInputStream.");

            this.hasWork = false;

            await Task.Run(() =>
            {
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

                    if (!this.inputStream.CanSeek)
                    {
                        throw new NotSupportedException(string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.StreamMustSupportSeekException,
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
#if DOTNET5_4
                        string filePath = fileLocation.FilePath;
                        if(Interop.CrossPlatformHelpers.IsWindows)
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
                            fileLocation.FilePath,
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
            });

            this.SharedTransferData.TotalLength = this.inputStream.Length;

            int count = (int)Math.Ceiling((double)(this.SharedTransferData.TotalLength - this.transferJob.CheckPoint.EntryTransferOffset) / this.SharedTransferData.BlockSize);

            if (null != this.transferJob.CheckPoint.TransferWindow)
            {
                count += this.transferJob.CheckPoint.TransferWindow.Count;
            }

            this.lastTransferWindow = new Queue<long>(this.transferJob.CheckPoint.TransferWindow);

            this.md5HashStream = new MD5HashStream(
                this.inputStream,
                this.transferJob.CheckPoint.EntryTransferOffset,
                true);

            this.PreProcessed = true;

            // This reader will come into 'Finish' state after all chunks are read and MD5 calculation completes.
            // So initialize the CountDownEvent to count (number of chunks to read) + 1 (md5 calculation).
            this.countdownEvent = new CountdownEvent(count + 1);

            if (0 != count)
            {
                // Change the state to 'ReadStream' before awaiting MD5 calculation task to not block the reader.
                this.state = State.ReadStream;
                this.hasWork = true;
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

            this.hasWork = false;

            byte[][] memoryBuffer = this.Scheduler.MemoryManager.RequireBuffers(this.SharedTransferData.MemoryChunksRequiredEachTime);

            if (null != memoryBuffer)
            {
                long startOffset = 0;

                if (0 != this.lastTransferWindow.Count)
                {
                    startOffset = this.lastTransferWindow.Dequeue();
                }
                else
                {
                    bool canRead = false;

                    lock (this.transferJob.CheckPoint.TransferWindowLock)
                    {
                        if (this.transferJob.CheckPoint.TransferWindow.Count < Constants.MaxCountInTransferWindow)
                        {
                            startOffset = this.transferJob.CheckPoint.EntryTransferOffset;

                            if (this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
                            {
                                this.transferJob.CheckPoint.TransferWindow.Add(startOffset);
                                this.transferJob.CheckPoint.EntryTransferOffset = Math.Min(
                                    this.transferJob.CheckPoint.EntryTransferOffset + this.SharedTransferData.BlockSize,
                                    this.SharedTransferData.TotalLength);

                                canRead = true;
                            }
                        }
                    }

                    if (!canRead)
                    {
                        this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                        this.hasWork = true;
                        return;
                    }
                }

                if ((startOffset > this.SharedTransferData.TotalLength)
                    || (startOffset < 0))
                {
                    this.Scheduler.MemoryManager.ReleaseBuffers(memoryBuffer);
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                ReadDataState asyncState = new ReadDataState
                {
                    MemoryBuffer = memoryBuffer,
                    BytesRead = 0,
                    StartOffset = startOffset,
                    Length = (int)Math.Min(this.SharedTransferData.BlockSize, this.SharedTransferData.TotalLength - startOffset),
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

            if (asyncState.BytesRead < asyncState.Length)
            {
                await this.ReadChunkAsync(asyncState);
            }
            else
            {
                this.Controller.CheckCancellation();

                if (!this.md5HashStream.MD5HashTransformBlock(asyncState.StartOffset, asyncState.MemoryBuffer, 0, asyncState.Length))
                {
                    // Error info has been set in Calculate MD5 action, just return
                    return;
                }

                TransferData transferData = new TransferData(this.Scheduler.MemoryManager)
                {
                    StartOffset = asyncState.StartOffset,
                    Length = asyncState.Length,
                    MemoryBuffer = asyncState.MemoryBuffer
                };

                asyncState.MemoryBuffer = null;

                this.SharedTransferData.AvailableData.TryAdd(transferData.StartOffset, transferData);

                this.SetChunkFinish();
            }
        }

        private void SetHasWork()
        {
            if (this.HasWork)
            {
                return;
            }

            // Check if we have blocks available to download.
            if ((null != this.lastTransferWindow && this.lastTransferWindow.Any())
                || this.transferJob.CheckPoint.EntryTransferOffset < this.SharedTransferData.TotalLength)
            {
                this.hasWork = true;
                return;
            }
        }

        private void SetChunkFinish()
        {
            if (this.countdownEvent.Signal())
            {
                this.state = State.Finished;
                this.CloseOwnStream();

                if (!this.md5HashStream.SucceededSeparateMd5Calculator)
                {
                    return;
                }

                var md5 = this.md5HashStream.MD5HashTransformFinalBlock();
                this.SharedTransferData.Attributes = new Attributes()
                {
                    ContentMD5 = md5,
                    OverWriteAll = false
                };
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
