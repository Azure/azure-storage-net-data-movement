//------------------------------------------------------------------------------
// <copyright file="RangeBasedWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    abstract class RangeBasedWriter : TransferReaderWriterBase
    {
        /// <summary>
        /// Keeps track of the internal state-machine state.
        /// </summary>
        private volatile State state;

        /// <summary>
        /// Countdown event to track number of chunks that still need to be
        /// uploaded/are in progress of being uploaded. Used to detect when
        /// all blocks have finished uploading and change state to Commit 
        /// state.
        /// </summary>
        private CountdownEvent toUploadChunksCountdownEvent;

        private volatile bool hasWork;

        protected RangeBasedWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.hasWork = true;
        }
        
        private enum State
        {
            FetchAttributes,
            Create,
            Upload,
            Commit,
            Error,
            Finished
        };

        public override bool IsFinished
        {
            get
            {
                return State.Error == this.state || State.Finished == this.state;
            }
        }

        public override bool HasWork
        {
            get
            {
                return this.hasWork &&
                    (!this.PreProcessed
                    || ((State.Upload == this.state) && this.SharedTransferData.AvailableData.Any())
                    || ((State.Commit == this.state) && (null != this.SharedTransferData.Attributes)));
            }
        }

        protected TransferJob TransferJob
        {
            get 
            {
                return this.SharedTransferData.TransferJob;
            }
        }

        protected abstract Uri DestUri
        {
            get;
        }

        public override async Task DoWorkInternalAsync()
        {
            switch (this.state)
            { 
                case State.FetchAttributes:
                    await this.FetchAttributesAsync();
                    break;
                case State.Create:
                    await this.CreateAsync();
                    break;
                case State.Upload:
                    await this.UploadAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
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
                if (null != this.toUploadChunksCountdownEvent)
                {
                    this.toUploadChunksCountdownEvent.Dispose();
                    this.toUploadChunksCountdownEvent = null;
                }
            }
        }
        
        private async Task FetchAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchAttributes,
                "FetchAttributesAsync called, but state isn't FetchAttributes",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            this.CheckInputStreamLength(this.SharedTransferData.TotalLength);

            bool exist = !this.Controller.IsForceOverwrite;

            if (!this.Controller.IsForceOverwrite)
            {
                try
                {
                    await Utils.ExecuteXsclApiCallAsync(
                        async () => await this.DoFetchAttributesAsync(),
                        this.CancellationToken);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
                {
                    var se = e as StorageException ?? e.InnerException as StorageException;
#else
                catch (StorageException se)
                {
#endif
                    // Getting a storage exception is expected if the file doesn't
                    // exist. In this case we won't error out, but set the 
                    // exist flag to false to indicate we're uploading
                    // a new file instead of overwriting an existing one.
                    if (null != se.RequestInformation &&
                        se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        exist = false;
                    }
                    else
                    {
                        this.HandleFetchAttributesResult(se);
                        throw;
                    }
                }
                catch (Exception e)
                {
                    this.HandleFetchAttributesResult(e);
                    throw;
                }

                await this.Controller.CheckOverwriteAsync(
                    exist,
                    this.TransferJob.Source.Instance,
                    this.TransferJob.Destination.Instance);
            }

            if (this.TransferJob.Destination.Type == TransferLocationType.AzureBlob)
            {
                (this.TransferJob.Destination as AzureBlobLocation).CheckedAccessCondition = true;
            }
            else
            {
                (this.TransferJob.Destination as AzureFileLocation).CheckedAccessCondition = true;
            }

            this.Controller.UpdateProgressAddBytesTransferred(0);

            SingleObjectCheckpoint checkpoint = this.TransferJob.CheckPoint;

            // We should create the destination if there's no previous transfer progress in the checkpoint.
            // Create will clear all data if the destination already exists.
            bool shouldCreate = (checkpoint.EntryTransferOffset == 0) && (!checkpoint.TransferWindow.Any());

            if (!shouldCreate)
            {
                this.InitUpload();
            }
            else
            {
                this.state = State.Create;
            }

            this.hasWork = true;
        }

        private async Task CreateAsync()
        {
            Debug.Assert(
                this.state == State.Create,
                "CreateAsync called, but state isn't Create",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            await Utils.ExecuteXsclApiCallAsync(
                async () => await this.DoCreateAsync(this.SharedTransferData.TotalLength),
                this.CancellationToken);

            this.InitUpload();
        }

        private void InitUpload()
        {
            Debug.Assert(
                null == this.toUploadChunksCountdownEvent,
                "toUploadChunksCountdownEvent expected to be null");

            if ((this.TransferJob.CheckPoint.EntryTransferOffset != this.SharedTransferData.TotalLength)
                && (0 != this.TransferJob.CheckPoint.EntryTransferOffset % this.SharedTransferData.BlockSize))
            {
                throw new FormatException(Resources.RestartableInfoCorruptedException);
            }

            // Calculate number of chunks.
            int numChunks = (int)Math.Ceiling(
                (this.SharedTransferData.TotalLength - this.TransferJob.CheckPoint.EntryTransferOffset) / (double)this.SharedTransferData.BlockSize)
                + this.TransferJob.CheckPoint.TransferWindow.Count;

            if (0 == numChunks)
            {
                this.PreProcessed = true;
                this.SetCommit();
            }
            else
            {
                this.toUploadChunksCountdownEvent = new CountdownEvent(numChunks);

                this.state = State.Upload;
                this.PreProcessed = true;
                this.hasWork = true;
            }
        }

        private async Task UploadAsync()
        {
            Debug.Assert(
                State.Upload == this.state || State.Error == this.state,
                "UploadAsync called, but state isn't Upload",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            Debug.Assert(
                null != this.toUploadChunksCountdownEvent,
                "toUploadChunksCountdownEvent not expected to be null");

            if (State.Error == this.state)
            {
                // Some thread has set the error message, just return here.
                return;
            }

            TransferData transferData = this.GetFirstAvailable();

            this.hasWork = true;

            await Task.Yield();

            if (null != transferData)
            {
                using (transferData)
                {
                    await this.UploadChunkAsync(transferData).ConfigureAwait(false);
                }
            }
        }

        private async Task UploadChunkAsync(TransferData transferData)
        {
            Debug.Assert(null != transferData, "transferData object expected");
            Debug.Assert(
                this.state == State.Upload || this.state == State.Error,
                "UploadChunkAsync called, but state isn't Upload or Error",
                "Current state is {0}",
                this.state);

            // If a parallel operation caused the controller to be placed in
            // error state exit early to avoid unnecessary I/O.
            if (this.state == State.Error)
            {
                return;
            }

            bool allZero = true;

            for (int i = 0; i < transferData.MemoryBuffer.Length; ++i)
            {
                var memoryChunk = transferData.MemoryBuffer[i];
                for (int j = 0; j < memoryChunk.Length; ++j)
                {
                    if (0 != memoryChunk[j])
                    {
                        allZero = false;
                        break;
                    }
                }

                if (!allZero)
                {
                    break;
                }
            }
            this.Controller.CheckCancellation();

            if (!allZero)
            {
                if (transferData.MemoryBuffer.Length == 1)
                {
                    transferData.Stream = new MemoryStream(transferData.MemoryBuffer[0], 0, transferData.Length);
                }
                else
                {
                    transferData.Stream = new ChunkedMemoryStream(transferData.MemoryBuffer, 0, transferData.Length);
                }

                await this.WriteRangeAsync(transferData).ConfigureAwait(false);
            }

            this.FinishChunk(transferData);
        }

        private void FinishChunk(TransferData transferData)
        {
            Debug.Assert(null != transferData, "transferData object expected");
            Debug.Assert(
                this.state == State.Upload || this.state == State.Error,
                "FinishChunk called, but state isn't Upload or Error",
                "Current state is {0}",
                this.state);

            // If a parallel operation caused the controller to be placed in
            // error state exit, make sure not to accidentally change it to
            // the Commit state.
            if (this.state == State.Error)
            {
                return;
            }

            this.Controller.UpdateProgress(() =>
            {
                lock (this.TransferJob.CheckPoint.TransferWindowLock)
                {
                    this.TransferJob.CheckPoint.TransferWindow.Remove(transferData.StartOffset);
                }
                this.SharedTransferData.TransferJob.Transfer.UpdateJournal();

                this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
            });

            if (this.toUploadChunksCountdownEvent.Signal())
            {
                this.SetCommit();
            }
        }

        private void SetCommit()
        {
            this.state = State.Commit;
            this.hasWork = true;
        }
        
        private async Task CommitAsync()
        {
            Debug.Assert(
                this.state == State.Commit,
                "CommitAsync called, but state isn't Commit",
                "Current state is {0}",
                this.state);

            this.hasWork = false;

            await Utils.ExecuteXsclApiCallAsync(
                async () => await this.DoCommitAsync(),
                this.CancellationToken);
            
            this.SetFinished();
        }

        private void SetFinished()
        {
            this.state = State.Finished;
            this.hasWork = false;

            this.NotifyFinished(null);
        }

        protected abstract void CheckInputStreamLength(long streamLength);

        protected abstract Task DoFetchAttributesAsync();

        protected abstract void HandleFetchAttributesResult(Exception e);

        protected abstract Task DoCreateAsync(long size);

        protected abstract Task WriteRangeAsync(TransferData transferData);
        
        protected abstract Task DoCommitAsync();
    }
}
