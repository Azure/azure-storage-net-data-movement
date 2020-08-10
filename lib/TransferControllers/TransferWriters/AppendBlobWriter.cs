//------------------------------------------------------------------------------
// <copyright file="AppendBlobWriter.cs" company="Microsoft">
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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.Blob.Protocol;

    internal sealed class AppendBlobWriter : TransferReaderWriterBase
    {
        private volatile State state;
        private AzureBlobLocation destLocation;
        private CloudAppendBlob appendBlob;
        private long expectedOffset = 0;

        /// <summary>
        /// Work token indicates whether this writer has work, could be 0(no work) or 1(has work).
        /// </summary>
        private volatile int workToken;

        /// <summary>
        /// To indicate whether the destination already exist before this writing.
        /// If no, when try to set destination's attribute, should get its attributes first.
        /// </summary>
        private bool destExist = false;

        public AppendBlobWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.SharedTransferData.TransferJob.Destination as AzureBlobLocation;
            this.appendBlob = this.destLocation.Blob as CloudAppendBlob;

            Debug.Assert(null != this.appendBlob, "The destination is not an append blob while initializing a AppendBlobWriter instance.");

            this.state = State.FetchAttributes;
            this.workToken = 1;
        }

        public override bool HasWork
        {
            get 
            {
                return this.workToken == 1 &&
                    ((State.FetchAttributes == this.state) ||
                    (State.Create == this.state) ||
                    (State.UploadBlob == this.state && this.SharedTransferData.AvailableData.ContainsKey(this.expectedOffset)) ||
                    (State.Commit == this.state && null != this.SharedTransferData.Attributes));
            }
        }

        public override bool IsFinished
        {
            get
            {
                return State.Error == this.state || State.Finished == this.state;
            }
        }

        private enum State
        {
            FetchAttributes,
            Create,
            UploadBlob,
            Commit,
            Error,
            Finished
        };

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
                case State.UploadBlob:
                    await this.UploadBlobAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
                    break;
                case State.Error:
                default:
                    break;
            }
        }

        private async Task FetchAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchAttributes,
                "FetchAttributesAsync called, but state isn't FetchAttributes",
                "Current state is {0}",
                this.state);

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            if (this.SharedTransferData.TotalLength > Constants.MaxAppendBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SharedTransferData.TotalLength),
                            Resources.AppendBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxAppendBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            bool existingBlob = !this.Controller.IsForceOverwrite;

            if (!this.Controller.IsForceOverwrite)
            {
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    this.destLocation.AccessCondition,
                    this.destLocation.CheckedAccessCondition);

                try
                {
                    await Utils.ExecuteXsclApiCallAsync(
                        async () => await this.appendBlob.FetchAttributesAsync(
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
                            this.CancellationToken),
                        this.CancellationToken);

                    this.destExist = true;
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
                {
                    var se = e as StorageException ?? e.InnerException as StorageException;
#else
                catch (StorageException se)
                {
#endif
                    // Getting a storage exception is expected if the blob doesn't
                    // exist. In this case we won't error out, but set the 
                    // existingBlob flag to false to indicate we're uploading
                    // a new blob instead of overwriting an existing blob.
                    if (null != se.RequestInformation &&
                        se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        existingBlob = false;
                    }
                    else if (null != se &&
                        (0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase)))
                    {
                        throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            await this.HandleFetchAttributesResultAsync(existingBlob);
        }

        private async Task HandleFetchAttributesResultAsync(bool existingBlob)
        {
            this.destLocation.CheckedAccessCondition = true;

            if (!this.Controller.IsForceOverwrite)
            {
                // If destination file exists, query user whether to overwrite it.
                await this.Controller.CheckOverwriteAsync(
                    existingBlob,
                    this.SharedTransferData.TransferJob.Source.Instance,
                    this.appendBlob);
            }

            this.Controller.UpdateProgressAddBytesTransferred(0);

            if (existingBlob)
            {
                if (this.appendBlob.Properties.BlobType == BlobType.Unspecified)
                {
                    throw new InvalidOperationException(Resources.FailedToGetBlobTypeException);
                }

                if (this.appendBlob.Properties.BlobType != BlobType.AppendBlob)
                {
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch);
                }
            }

            // We do check point consistency validation in reader, so directly use it here.
            SingleObjectCheckpoint checkpoint = this.SharedTransferData.TransferJob.CheckPoint;

            if ((null != checkpoint.TransferWindow)
                && (checkpoint.TransferWindow.Any()))
            {
                checkpoint.TransferWindow.Sort();
                this.expectedOffset = checkpoint.TransferWindow[0];
            }
            else
            {
                this.expectedOffset = checkpoint.EntryTransferOffset;
            }

            if (0 == this.expectedOffset)
            {
                this.state = State.Create;
            }
            else
            {
                if (!this.Controller.IsForceOverwrite && !existingBlob)
                {
                    throw new TransferException(Resources.DestinationChangedException);
                }

                this.PreProcessed = true;

                if (this.expectedOffset == this.SharedTransferData.TotalLength)
                {
                    this.state = State.Commit;
                }
                else
                {
                    this.state = State.UploadBlob;
                }
            }

            this.workToken = 1;
        }

        private async Task CreateAsync()
        {
            Debug.Assert(State.Create == this.state, "Calling CreateAsync, state should be Create");

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                 this.destLocation.AccessCondition,
                 true);

            if (this.destExist)
            { 
                this.CleanupPropertyForCanonicalization();
            }

            await Utils.ExecuteXsclApiCallAsync(
                async () => await this.appendBlob.CreateOrReplaceAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions, true),
                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                    this.CancellationToken),
                this.CancellationToken);

            this.PreProcessed = true;

            if (this.expectedOffset == this.SharedTransferData.TotalLength)
            {
                this.state = State.Commit;
            }
            else
            {
                this.state = State.UploadBlob;
            }

            this.workToken = 1;
        }

        private async Task UploadBlobAsync()
        {
            Debug.Assert(State.UploadBlob == this.state, "Calling UploadBlobAsync, state should be UploadBlob");

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            TransferData transferData = null;
            if (!this.SharedTransferData.AvailableData.TryRemove(this.expectedOffset, out transferData))
            {
                this.workToken = 1;
                return;
            }

            if (null != transferData)
            {
                using (transferData)
                {
                    long currentOffset = this.expectedOffset;
                    if (0 != transferData.Length)
                    {
                        this.expectedOffset += transferData.Length;

                        if (transferData.MemoryBuffer.Length == 1)
                        {
                            transferData.Stream = new MemoryStream(transferData.MemoryBuffer[0], 0, transferData.Length);
                        }
                        else
                        {
                            transferData.Stream = new ChunkedMemoryStream(transferData.MemoryBuffer, 0, transferData.Length);
                        }

                        AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true) ?? new AccessCondition();
                        accessCondition.IfAppendPositionEqual = currentOffset;

                        bool needToCheckContent = false;
                        StorageException catchedStorageException = null;


                        try
                        {
                            await Utils.ExecuteXsclApiCallAsync(
                                async () => await this.appendBlob.AppendBlockAsync(
                                    transferData.Stream,
                                    null,
                                    accessCondition,
                                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                                    this.CancellationToken),
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
                            if ((null != se.RequestInformation) &&
                                ((int)HttpStatusCode.PreconditionFailed == se.RequestInformation.HttpStatusCode) &&
                                (se.RequestInformation.ErrorCode == BlobErrorCodeStrings.InvalidAppendCondition))
                            {
                                needToCheckContent = true;
                                catchedStorageException = se;
                            }
                            else
                            {
                                throw;
                            }
                        }

                        if (needToCheckContent &&
                            (!await this.ValidateUploadedChunkAsync(transferData.MemoryBuffer, currentOffset, (long)transferData.Length)))
                        {
                            throw new InvalidOperationException(Resources.DestinationChangedException, catchedStorageException);
                        }
                    }

                    this.Controller.UpdateProgress(() =>
                        {
                            lock (this.SharedTransferData.TransferJob.CheckPoint.TransferWindowLock)
                            {
                                this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Remove(currentOffset);
                            }
                            this.SharedTransferData.TransferJob.Transfer.UpdateJournal();

                        // update progress
                        this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                        });

                    if (this.expectedOffset == this.SharedTransferData.TotalLength)
                    {
                        this.state = State.Commit;
                    }

                    this.workToken = 1;
                }
            }
        }

        /// <summary>
        /// Cleanup properties that might cause request canonicalization check failure.
        /// </summary>
        private void CleanupPropertyForCanonicalization()
        {
            this.appendBlob.Properties.ContentLanguage = null;
            this.appendBlob.Properties.ContentEncoding = null;
        }

        private async Task CommitAsync()
        {
            Debug.Assert(State.Commit == this.state, "Calling CommitAsync, state should be Commit");

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            if (!this.Controller.IsForceOverwrite && !this.destExist)
            {
                await Utils.ExecuteXsclApiCallAsync(
                    async () => await this.appendBlob.FetchAttributesAsync(
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken),
                    this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.appendBlob.Metadata);
            Utils.SetAttributes(this.appendBlob, this.SharedTransferData.Attributes);
            await this.Controller.SetCustomAttributesAsync(this.SharedTransferData.TransferJob.Source.Instance, this.appendBlob);

            await this.appendBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                operationContext,
                this.CancellationToken);

            if (!originalMetadata.DictionaryEquals(this.appendBlob.Metadata))
            {
                await this.appendBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
        }

        private async Task<bool> ValidateUploadedChunkAsync(byte[][] currentData, long startOffset, long length)
        {
            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);
            await this.appendBlob.FetchAttributesAsync(
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                operationContext,
                this.CancellationToken);

            this.destExist = true;

            if (this.appendBlob.Properties.Length != (startOffset + length))
            {
                return false;
            }

            if (length <= 0)
            {
                // Nothing to compare
                return true;
            }

            byte[] buffer = new byte[length];

            // Do not expect any exception here.
            await this.appendBlob.DownloadRangeToByteArrayAsync(
                buffer,
                0,
                startOffset,
                length,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                operationContext,
                this.CancellationToken);

            int index = 0;
            for (int i = 0; i < currentData.Length; i++)
            {
                for (int j = 0; j < currentData[i].Length; j++)
                {
                    if (currentData[i][j] != buffer[index++])
                    {
                        return false;
                    }

                    if (index == length)
                    {
                        // Reach to the end and nothing different
                        return true;
                    }
                }
            }

            return true;
        }

        private void SetFinish()
        {
            this.state = State.Finished;
            this.NotifyFinished(null);
            this.workToken = 0;
        }
    }
}
