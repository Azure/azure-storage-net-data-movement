//------------------------------------------------------------------------------
// <copyright file="AppendBlobWriter.cs" company="Microsoft">
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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;

    internal sealed class AppendBlobWriter : TransferReaderWriterBase
    {
        private volatile State state;
        private volatile bool hasWork;
        private AzureBlobLocation destLocation;
        private CloudAppendBlob appendBlob;
        private long expectedOffset = 0;

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
            this.hasWork = true;
        }

        public override bool HasWork
        {
            get 
            {
                return this.hasWork &&
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

            this.hasWork = false;

            if (this.SharedTransferData.TotalLength > Constants.MaxBlockBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SharedTransferData.TotalLength),
                            Resources.AppendBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxBlockBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            bool existingBlob = true;

            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                this.destLocation.AccessCondition,
                this.destLocation.CheckedAccessCondition);

            try
            {
                await this.appendBlob.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                    this.CancellationToken);

                this.destExist = true;
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception e) when (e is StorageException || e.InnerException is StorageException)
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
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch);
                }
                else
                {
                    throw;
                }
            }

            this.HandleFetchAttributesResult(existingBlob);
        }

        private void HandleFetchAttributesResult(bool existingBlob)
        {
            this.destLocation.CheckedAccessCondition = true;

            // If destination file exists, query user whether to overwrite it.
            this.Controller.CheckOverwrite(
                existingBlob,
                this.SharedTransferData.SourceLocation,
                this.appendBlob.Uri.ToString());

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
                if (!existingBlob)
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

            this.hasWork = true;
        }

        private async Task CreateAsync()
        {
            Debug.Assert(State.Create == this.state, "Calling CreateAsync, state should be Create");

            this.hasWork = false; 
            
            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                 this.destLocation.AccessCondition,
                 true);

            await this.appendBlob.CreateOrReplaceAsync(
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
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

            this.hasWork = true;
        }

        private async Task UploadBlobAsync()
        {
            Debug.Assert(State.UploadBlob == this.state, "Calling UploadBlobAsync, state should be UploadBlob");

            this.hasWork = false;

            TransferData transferData = null;
            if (!this.SharedTransferData.AvailableData.TryRemove(this.expectedOffset, out transferData))
            {
                this.hasWork = true;
                return;
            }

            if (null != transferData)
            {
                using (transferData)
                {
                    long currentOffset = this.expectedOffset;
                    this.expectedOffset += transferData.Length;

                    transferData.Stream = new MemoryStream(transferData.MemoryBuffer, 0, transferData.Length);

                    AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true) ?? new AccessCondition();
                    accessCondition.IfAppendPositionEqual = currentOffset;

                    bool needToCheckContent = false;

                    try
                    {
                        await this.appendBlob.AppendBlockAsync(transferData.Stream,
                            null,
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
                            this.CancellationToken);
                    }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                    catch (Exception e) when (e is StorageException || e.InnerException is StorageException)
                    {
                        var se = e as StorageException ?? e.InnerException as StorageException;
#else
                    catch (StorageException se)
                    {
#endif
                        if ((null != se.RequestInformation) &&
                            ((int)HttpStatusCode.PreconditionFailed == se.RequestInformation.HttpStatusCode) &&
                            (null != se.RequestInformation.ExtendedErrorInformation) &&
                            (se.RequestInformation.ExtendedErrorInformation.ErrorCode == BlobErrorCodeStrings.InvalidAppendCondition))
                        {
                            needToCheckContent = true;
                        }
                        else
                        {
                            throw;
                        }
                    }

                    if (needToCheckContent &&
                        (!await this.ValidateUploadedChunkAsync(transferData.MemoryBuffer, currentOffset, (long)transferData.Length)))
                    {
                        throw new InvalidOperationException(Resources.DestinationChangedException);
                    }

                    this.Controller.UpdateProgress(() =>
                    {
                        lock (this.SharedTransferData.TransferJob.CheckPoint.TransferWindowLock)
                        {
                            this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Remove(currentOffset);
                        }

                        // update progress
                        this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                    });

                    if (this.expectedOffset == this.SharedTransferData.TotalLength)
                    {
                        this.state = State.Commit;
                    }

                    this.hasWork = true;
                }
            }
        }

        private async Task CommitAsync()
        {
            Debug.Assert(State.Commit == this.state, "Calling CommitAsync, state should be Commit");

            this.hasWork = false;

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            if (!this.destExist)
            {
                await this.appendBlob.FetchAttributesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.appendBlob.Metadata);
            Utils.SetAttributes(this.appendBlob, this.SharedTransferData.Attributes);

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

        private async Task<bool> ValidateUploadedChunkAsync(byte[] currentData, long startOffset, long length)
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

            for (int i = 0; i < length; ++i)
            {
                if (currentData[i] != buffer[i])
                {
                    return false;
                }
            }

            return true;
        }

        private void SetFinish()
        {
            this.state = State.Finished;
            this.NotifyFinished(null);
            this.hasWork = false;
        }
    }
}
