//------------------------------------------------------------------------------
// <copyright file="StreamedWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class StreamedWriter : TransferReaderWriterBase, IDisposable
    {
        /// <summary>
        /// Streamed destination is written sequentially. 
        /// This variable records offset of next chunk to be written.
        /// </summary>
        private long expectOffset = 0;

        /// <summary>
        /// Value to indicate whether there's work to do in the writer.
        /// </summary>
        private volatile bool hasWork;

        /// <summary>
        /// Stream to calculation destination's content MD5.
        /// </summary>
        private MD5HashStream md5HashStream;

        private Stream outputStream;

        /// <summary>
        /// Value to indicate whether the stream is a file stream opened by the writer or input by user.
        /// If it's a file stream opened by the writer, we should closed it after transferring finished.
        /// </summary>
        private bool ownsStream;

        private string filePath = null;

        private string longFilePath = null;

        private volatile State state;

        private volatile bool isStateSwitchedInternal;

        public StreamedWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.hasWork = true;
            this.state = State.OpenOutputStream;
        }

        private enum State
        {
            OpenOutputStream,
            CalculateMD5,
            Write,
            Error,
            Finished
        };

        private TransferJob TransferJob
        {
            get
            {
                return this.SharedTransferData.TransferJob;
            }
        }

        public override bool HasWork
        {
            get
            {
                return this.hasWork &&
                    ((State.OpenOutputStream == this.state)
                    || (State.CalculateMD5 == this.state)
                    || ((State.Write == this.state)
                        && ((this.SharedTransferData.TotalLength == this.expectOffset) || this.SharedTransferData.AvailableData.ContainsKey(this.expectOffset))));
            }
        }

        public override bool IsFinished
        {
            get
            {
                return State.Error == this.state || State.Finished == this.state;
            }
        }

        public override async Task DoWorkInternalAsync()
        {
            switch (this.state)
            {
                case State.OpenOutputStream:
                    await this.HandleOutputStreamAsync();
                    break;
                case State.CalculateMD5:
                    await this.CalculateMD5Async();
                    break;
                case State.Write:
                    await this.WriteChunkDataAsync();
                    break;
                default:
                    break;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                this.CloseOwnedOutputStream();
            }
        }

        private async Task HandleOutputStreamAsync()
        {
            this.hasWork = false;

            await Task.Run(async () =>
            {
                // Only do calculation related to transfer window when the file contains multiple chunks.
                if (!this.EnableOneChunkFileOptimization)
                {
                    // We do check point consistancy validation in reader, and directly use it in writer.
                    if ((null != this.TransferJob.CheckPoint.TransferWindow)
                        && this.TransferJob.CheckPoint.TransferWindow.Any())
                    {
                        this.TransferJob.CheckPoint.TransferWindow.Sort();
                        this.expectOffset = this.TransferJob.CheckPoint.TransferWindow[0];
                    }
                    else
                    {
                        this.expectOffset = this.TransferJob.CheckPoint.EntryTransferOffset;
                    }
                }

                if (TransferLocationType.Stream == this.TransferJob.Destination.Type)
                {
                    Stream streamInDestination = (this.TransferJob.Destination as StreamLocation).Stream;
                    if (!streamInDestination.CanWrite)
                    {
                        throw new NotSupportedException(string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.StreamMustSupportWriteException,
                            "outputStream"));
                    }

                    if (!streamInDestination.CanSeek)
                    {
                        throw new NotSupportedException(string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.StreamMustSupportSeekException,
                            "outputStream"));
                    }

                    this.outputStream = streamInDestination;
                }
                else
                {
                    this.filePath = (this.TransferJob.Destination as FileLocation).FilePath;
                    this.longFilePath = this.filePath.ToLongPath();

                    if (!this.Controller.IsForceOverwrite)
                    {
                        await this.Controller.CheckOverwriteAsync(
                            LongPathFile.Exists(this.longFilePath),
                            this.TransferJob.Source.Instance,
                            this.filePath);
                    }

                    this.Controller.CheckCancellation();

                    try
                    {
                        FileMode fileMode = 0 == this.expectOffset ? FileMode.OpenOrCreate : FileMode.Open;

#if DOTNET5_4

                        // Attempt to open the file first so that we throw an exception before getting into the async work
                        this.outputStream = new FileStream(
                            this.longFilePath,
                            fileMode,
                            FileAccess.ReadWrite,
                            FileShare.None);
#else
                        this.outputStream = LongPathFile.Open(
                            this.longFilePath, 
                            fileMode, 
                            FileAccess.ReadWrite, 
                            FileShare.None);
#endif

                        this.ownsStream = true;
                    }
                    catch (Exception ex)
                    {
                        string exceptionMessage = string.Format(
                                    CultureInfo.CurrentCulture,
                                    Resources.FailedToOpenFileException,
                                    this.filePath,
                                    ex.Message);

                        throw new TransferException(
                                TransferErrorCode.OpenFileFailed,
                                exceptionMessage,
                                ex);
                    }
                }

                this.outputStream.SetLength(this.SharedTransferData.TotalLength);

                this.md5HashStream = new MD5HashStream(
                    this.outputStream,
                    this.expectOffset,
                    !this.SharedTransferData.DisableContentMD5Validation, 
                    TransferJob.Transfer.Logger);

                if (this.md5HashStream.FinishedSeparateMd5Calculator)
                {
                    this.state = State.Write;
                }
                else
                {
                    this.state = State.CalculateMD5;
                }

                this.PreProcessed = true;

                // Switch state internal for one chunk small file.
                if (this.EnableOneChunkFileOptimization &&
                    State.Write == this.state &&
                    ((this.SharedTransferData.TotalLength == this.expectOffset) || this.SharedTransferData.AvailableData.ContainsKey(this.expectOffset)))
                {
                    this.isStateSwitchedInternal = true;
                    await this.WriteChunkDataAsync().ConfigureAwait(false);
                }
                else
                {
                    this.hasWork = true;
                }
            });
        }

        private Task CalculateMD5Async()
        {
            Debug.Assert(
                this.state == State.CalculateMD5,
                "GetCalculateMD5Action called, but state isn't CalculateMD5",
                "Current state is {0}",
                this.state);

            this.state = State.Write;
            this.hasWork = true;

            return Task.Run(
                delegate
                {
                    this.md5HashStream.CalculateMd5(this.Scheduler.MemoryManager, this.Controller.CheckCancellation);
                });
        }

        private async Task WriteChunkDataAsync()
        {
            Debug.Assert(
                this.state == State.Write || this.state == State.Error,
                "WriteChunkDataAsync called, but state isn't Write or Error",
                "Current state is {0}",
                this.state);
            
            if (!this.isStateSwitchedInternal)
            {
                this.hasWork = false;
            }
            
            long currentWriteOffset = this.expectOffset;
            TransferData transferData;
            if (this.SharedTransferData.AvailableData.TryRemove(this.expectOffset, out transferData))
            {
                this.expectOffset = Math.Min(this.expectOffset + transferData.Length, this.SharedTransferData.TotalLength);
            }
            else
            {
                await this.SetHasWorkOrFinished().ConfigureAwait(false);
                return;
            }

            Debug.Assert(null != transferData, "TransferData in available data should not be null");
            Debug.Assert(currentWriteOffset == transferData.StartOffset, "StartOffset of TransferData in available data should be the same with the key.");

            try
            {
                await this.md5HashStream.WriteAsync(
                    currentWriteOffset,
                    transferData.MemoryBuffer,
                    0,
                    transferData.Length,
                    this.CancellationToken).ConfigureAwait(false);

                // If MD5HashTransformBlock returns false, it means some error happened in md5HashStream to calculate MD5.
                // then exception was already thrown out there, don't do anything more here.
                if (!this.md5HashStream.MD5HashTransformBlock(
                    transferData.StartOffset,
                    transferData.MemoryBuffer,
                    0,
                    transferData.Length))
                {
                    return;
                }
            }
            finally
            {
                this.Scheduler.MemoryManager.ReleaseBuffers(transferData.MemoryBuffer);
            }

            // Skip transfer window calculation and related journal recording operations when it's file with only one chunk.
            if (this.EnableOneChunkFileOptimization)
            {
                this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
            }
            else
            {
                int blockSize = this.SharedTransferData.BlockSize;
                long chunkStartOffset = (currentWriteOffset / blockSize) * blockSize;

                this.Controller.UpdateProgress(() =>
                {
                    if ((currentWriteOffset + transferData.Length) >= Math.Min(chunkStartOffset + blockSize, this.SharedTransferData.TotalLength))
                    {
                        lock (this.TransferJob.CheckPoint.TransferWindowLock)
                        {
                            if ((currentWriteOffset + transferData.Length) >= Math.Min(chunkStartOffset + blockSize, this.SharedTransferData.TotalLength))
                            {
                                this.TransferJob.CheckPoint.TransferWindow.Remove(chunkStartOffset);
                            }
                        }

                        this.SharedTransferData.TransferJob.Transfer.UpdateJournal();
                    }

                    this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                });
            }

            await this.SetHasWorkOrFinished().ConfigureAwait(false);
        }

        private async Task SetHasWorkOrFinished()
        {
            if (this.expectOffset == this.SharedTransferData.TotalLength)
            {
                Exception ex = null;
                if (this.md5HashStream.CheckMd5Hash && this.md5HashStream.SucceededSeparateMd5Calculator)
                {
                    string calculatedMd5 = this.md5HashStream.MD5HashTransformFinalBlock();
                    string storedMd5 = this.SharedTransferData.Attributes.ContentMD5;

                    if (!IsEqualOrEmpty(calculatedMd5, storedMd5))
                    {
                        var exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.DownloadedMd5MismatchException, calculatedMd5,
                            storedMd5);

                        ex = new TransferException(
                            TransferErrorCode.ContentChecksumMismatch,
                            exceptionMessage);

                        ex.Data.Add("source", this.TransferJob.Source);
                        ex.Data.Add("destination", this.TransferJob.Destination);
                        ex.Data.Add("calculatedMd5", calculatedMd5);
                        ex.Data.Add("storedMd5", storedMd5);
                    }
                }

                this.CloseOwnedOutputStream();

                if (this.TransferJob.Transfer.PreserveSMBAttributes)
                {
                    if (this.SharedTransferData.Attributes.CloudFileNtfsAttributes.HasValue
                        && !string.IsNullOrEmpty(this.longFilePath))
                    {
                        LongPathFile.SetFileTime(this.longFilePath, this.SharedTransferData.Attributes.CreationTime.Value, this.SharedTransferData.Attributes.LastWriteTime.Value);
                        LongPathFile.SetAttributes(this.longFilePath, Utils.AzureFileNtfsAttributesToLocalAttributes(this.SharedTransferData.Attributes.CloudFileNtfsAttributes.Value));
                    }
                }

                if ((PreserveSMBPermissions.None != this.TransferJob.Transfer.PreserveSMBPermissions)
                    && (!string.IsNullOrEmpty(this.SharedTransferData.Attributes.PortableSDDL)))
                {
                    if (!string.IsNullOrEmpty(this.longFilePath))
                    {
                        FileSecurityOperations.SetFileSecurity(
                            this.longFilePath,
                            this.SharedTransferData.Attributes.PortableSDDL,
                            this.TransferJob.Transfer.PreserveSMBPermissions);
                    }
                }

                await this.Controller.SetCustomAttributesAsync(this.TransferJob.Source.Instance, this.filePath)
                    .ConfigureAwait(false);

                this.NotifyFinished(ex);
                this.state = State.Finished;
            }
            else
            {
                this.hasWork = true;
            }
        }

        private bool IsEqualOrEmpty(string calculatedMd5, string storedMd5)
        {
            // if there is no md5 stored on server side there is nothing to compare.
            // We accept the risk and treat given file in the same way as the file with equal checksums
            if (string.IsNullOrEmpty(storedMd5))
            {
                TransferJob.Transfer.Logger.Warning("MD5 checksum stored on server is empty. Calculated value check will be omitted.");
                return true;
            }

            return calculatedMd5.Equals(storedMd5);
        }

        private void CloseOwnedOutputStream()
        {
            if (null != this.md5HashStream)
            {
                this.md5HashStream.Dispose();
                this.md5HashStream = null;
            }

            if (this.ownsStream)
            {
                if (null != this.outputStream)
                {
#if DOTNET5_4
                    this.outputStream.Dispose();
#else
                    this.outputStream.Close();
#endif
                    this.outputStream = null;
                }
            }
        }
    }
}
