//------------------------------------------------------------------------------
// <copyright file="BlockBlobWriter.cs" company="Microsoft">
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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal sealed class BlockBlobWriter : TransferReaderWriterBase
    {
        private volatile State state;
        private SortedDictionary<int, string> blockIds;
        private object blockIdsLock = new object();
        private readonly AzureBlobLocation destLocation;
        private readonly CloudBlockBlob blockBlob;
        private long uploadedLength = 0;

        /// <summary>
        /// Work token indicates whether this writer has work, could be 0(no work) or 1(has work).
        /// </summary>
        private volatile int workToken;

        public BlockBlobWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.SharedTransferData.TransferJob.Destination as AzureBlobLocation;
            this.blockBlob = this.destLocation.Blob as CloudBlockBlob;

            Debug.Assert(null != this.blockBlob, "The destination is not a block blob while initializing a BlockBlobWriter instance.");

            this.state = State.FetchAttributes;
            this.workToken = 1;
        }

        private enum State
        {
            FetchAttributes,
            // Path 1: FetchAttributes -> UploadBlob -> Commit -> Finished
            UploadBlob,
            Commit,
            // Path 2: FetchAttributes -> UploadBlobAndSetAttributes -> Finished
            UploadBlobAndSetAttributes,
            Error,
            Finished
        };

        public override bool PreProcessed
        {
            get;
            protected set;
        }

        public override bool HasWork => this.workToken == 1 &&
                                        (!this.PreProcessed
                                         || ((this.state == State.UploadBlob) && this.SharedTransferData.AvailableData.Any())
                                         || ((this.state == State.Commit) && (null != this.SharedTransferData.Attributes))
                                         || ((this.state == State.UploadBlobAndSetAttributes) && this.SharedTransferData.AvailableData.Any() && null != this.SharedTransferData.Attributes));

        public override bool IsFinished => State.Error == this.state || State.Finished == this.state;

        public override async Task DoWorkInternalAsync()
        {
            switch (this.state)
            {
                case State.FetchAttributes:
                    await this.FetchAttributesAsync();
                    break;
                case State.UploadBlob:
                    await this.UploadBlobAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
                    break;
                case State.UploadBlobAndSetAttributes:
                    await this.UploadBlobAndSetAttributesAsync();
                    break;
                case State.Error:
                case State.Finished:
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

            await Task.Yield();

            if (this.SharedTransferData.TotalLength > Constants.MaxBlockBlobFileSize)
            {
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.BlobFileSizeTooLargeException,
                            Utils.BytesToHumanReadableSize(this.SharedTransferData.TotalLength),
                            Resources.BlockBlob,
                            Utils.BytesToHumanReadableSize(Constants.MaxBlockBlobFileSize));

                throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        exceptionMessage);
            }

            if (!this.Controller.IsForceOverwrite)
            {
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    this.destLocation.AccessCondition,
                    this.destLocation.CheckedAccessCondition);

                try
                {
                    await this.destLocation.Blob.FetchAttributesAsync(
                            accessCondition,
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
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
                    await this.HandleFetchAttributesResultAsync(se);
                    return;
                }
            }

            await this.HandleFetchAttributesResultAsync(null);
        }

        private async Task HandleFetchAttributesResultAsync(Exception e)
        {
            bool existingBlob = !this.Controller.IsForceOverwrite;

            if (null != e)
            {
                StorageException se = e as StorageException;

                if (null != se)
                {
                    // Getting a storage exception is expected if the blob doesn't
                    // exist. In this case we won't error out, but set the 
                    // existingBlob flag to false to indicate we're uploading
                    // a new blob instead of overwriting an existing blob.
                    if (null != se.RequestInformation &&
                        se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        existingBlob = false;
                    }
                    else if ((0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase)))
                    {
                        throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
                    }
                    else
                    {
                        throw se;
                    }
                }
                else
                {
                    throw e;
                }
            }

            this.destLocation.CheckedAccessCondition = true;

            if (!this.Controller.IsForceOverwrite)
            {
                // If destination file exists, query user whether to overwrite it.
                await this.Controller.CheckOverwriteAsync(
                    existingBlob,
                    this.SharedTransferData.TransferJob.Source.Instance,
                    this.destLocation.Blob);
            }

            if (existingBlob)
            {
                if (this.destLocation.Blob.Properties.BlobType == BlobType.Unspecified)
                {
                    throw new InvalidOperationException(Resources.FailedToGetBlobTypeException);
                }

                if (this.destLocation.Blob.Properties.BlobType != BlobType.BlockBlob)
                {
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch);
                }

                Debug.Assert(
                    this.destLocation.Blob.Properties.BlobType == BlobType.BlockBlob,
                    "BlobType should be BlockBlob if we reach here.");
            }

            // Only do calculation related to transfer window when the file contains multiple chunks.
            if (!this.EnableOneChunkFileOptimization)
            {
                var checkpoint = this.SharedTransferData.TransferJob.CheckPoint;

                checkpoint.TransferWindow.Sort();

                this.uploadedLength = checkpoint.EntryTransferOffset;

                if (checkpoint.TransferWindow.Any())
                {
                    // The size of last block can be smaller than BlockSize.
                    this.uploadedLength -= Math.Min(checkpoint.EntryTransferOffset - checkpoint.TransferWindow.Last(), this.SharedTransferData.BlockSize);
                    this.uploadedLength -= (checkpoint.TransferWindow.Count - 1) * this.SharedTransferData.BlockSize;
                }
            }
            
            var singlePutBlobSizeThreshold = Math.Min(this.SharedTransferData.BlockSize, Constants.MaxSinglePutBlobSize);

            if (this.SharedTransferData.TotalLength > 0
                && this.SharedTransferData.TotalLength <= singlePutBlobSizeThreshold)
            {
                this.PrepareForPutBlob();
            }
            else
            {
                this.PrepareForPutBlockAndPutBlockList();
            }

            this.PreProcessed = true;
            this.workToken = 1;
        }

        private void PrepareForPutBlockAndPutBlockList()
        {
            if (string.IsNullOrEmpty(this.destLocation.BlockIdPrefix))
            {
                // BlockIdPrefix is never set before that this is the first time to transfer this file.
                // In block blob upload, it stores uploaded but not committed blocks on Azure Storage. 
                // In DM, we use block id to identify the blocks uploaded so we only need to upload it once.
                // Keep BlockIdPrefix in upload job object for restarting the transfer if anything happens.
                this.destLocation.BlockIdPrefix = this.GenerateBlockIdPrefix();
            }

            // Create sequence array.
            this.blockIds = new SortedDictionary<int, string>();
            this.InitializeBlockIds();

            this.state = State.UploadBlob;

            this.FinishBlock();
        }

        private void PrepareForPutBlob()
        {
            if (this.SharedTransferData.TotalLength == this.uploadedLength)
            {
                this.SetFinish();
            }
            else
            {
                this.state = State.UploadBlobAndSetAttributes;
            }
        }

        private string GenerateBlockIdPrefix()
        {
            // var blockIdPrefix = Guid.NewGuid().ToString("N") + "-";

            // Originally the blockId is an GUID + "-". It will cause some problem when switch machines or jnl get cleaned
            // to upload to the same block blob - block id is not shared between the 2 DMLib instances
            // and it may result in reaching the limitation of maximum 50000 uncommited blocks + 50000 committed blocks.
            // Change it to hash based prefix to make it preditable and can be shared between multiple DMLib instances
            string blobNameHash;
            using (var md5 = new MD5Wrapper())
            {
                var blobNameBytes = Encoding.UTF8.GetBytes(this.destLocation.Blob.Name);
                md5.UpdateHash(blobNameBytes, 0, blobNameBytes.Length);
                blobNameHash = md5.ComputeHash();
            }

            // The original GUID format prefix's length is 32 + 1 ("-")
            // As the service requires the blockid has the same size of each block,
            // To keep the compatibility, add 9 chars to the end of the hash ( 33 - 24)
            var blockIdPrefix = blobNameHash + "12345678-";
            return blockIdPrefix;
        }

        private async Task UploadBlobAsync()
        {
            Debug.Assert(
                State.UploadBlob == this.state || State.Error == this.state,
                "UploadBlobAsync called but state is not UploadBlob nor Error.",
                "Current state is {0}",
                this.state);
            TransferData transferData = this.GetFirstAvailable();

            await Task.Yield();

            if (null != transferData)
            {
                using (transferData)
                {
                    if (0 != transferData.Length)
                    {
                        if (transferData.MemoryBuffer.Length == 1)
                        {
                            transferData.Stream = new MemoryStream(transferData.MemoryBuffer[0], 0, transferData.Length);
                        }
                        else
                        {
                            transferData.Stream = new ChunkedMemoryStream(transferData.MemoryBuffer, 0, transferData.Length);
                        }

                        await this.blockBlob.PutBlockAsync(
                                this.GetBlockId(transferData.StartOffset),
                                transferData.Stream,
                                null,
                                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                                Utils.GenerateOperationContext(this.Controller.TransferContext),
                                this.CancellationToken);

                    }
                }

                // Skip transfer window calculation and related journal recording operations when it's file with only one chunk.
                if (this.EnableOneChunkFileOptimization)
                {
                    this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                }
                else
                {
                    this.Controller.UpdateProgress(() =>
                    {
                        lock (this.SharedTransferData.TransferJob.CheckPoint.TransferWindowLock)
                        {
                            this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Remove(transferData.StartOffset);
                        }

                        this.SharedTransferData.TransferJob.Transfer.UpdateJournal();

                    // update progress
                    this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                    });
                }

                Interlocked.Add(ref this.uploadedLength, transferData.Length);

                this.FinishBlock();
            }

            // Do not set workToken to 1 because it's always true in State.UploadBlob
            // Otherwise it may cause CommitAsync be called multiple times:
            // 1. UploadBlobAsync downloads all content, but doesn't set workToekn to 1 yet
            // 2. Call CommitAysnc, set workToken to 0
            // 3. UploadBlobAsync set workToken to 1.
            // 4. Call CommitAsync again since workToken is 1.
        }

        private async Task CommitAsync()
        {
            Debug.Assert(
                this.state == State.Commit,
                "CommitAsync called, but state isn't Commit",
                "Current state is {0}",
                this.state);

            if (Interlocked.CompareExchange(ref workToken, 0, 1) == 0)
            {
                return;
            }

            Utils.SetAttributes(this.blockBlob, this.SharedTransferData.Attributes);
            await this.Controller.SetCustomAttributesAsync(this.SharedTransferData.TransferJob.Source.Instance, this.blockBlob);

            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            await this.blockBlob.PutBlockListAsync(
                        this.blockIds.Values,
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                        blobRequestOptions,
                        operationContext,
                        this.CancellationToken);

            // REST API PutBlockList cannot clear existing Content-Type of block blob, so if it's needed to clear existing
            // Content-Type, REST API SetBlobProperties must be called explicitly:
            // 1. The attributes are inherited from others and Content-Type is null or empty.
            // 2. User specifies Content-Type to string.Empty while uploading.
            if ((this.SharedTransferData.Attributes.OverWriteAll && string.IsNullOrEmpty(this.blockBlob.Properties.ContentType))
                || (!this.SharedTransferData.Attributes.OverWriteAll && this.blockBlob.Properties.ContentType == string.Empty))
            {
                await this.blockBlob.SetPropertiesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            this.SetFinish();
        }

        private async Task UploadBlobAndSetAttributesAsync()
        {
            Debug.Assert(
                State.UploadBlobAndSetAttributes == this.state || State.Error == this.state,
                "UploadBlobAndSetAttributesAsync called but state is not UploadBlobAndSetAttributes nor Error.",
                "Current state is {0}",
                this.state);

            TransferData transferData = this.GetFirstAvailable();

            await Task.Yield();

            if (null != transferData)
            {
                using (transferData)
                {
                    if (transferData.MemoryBuffer.Length == 1)
                    {
                        transferData.Stream = new MemoryStream(transferData.MemoryBuffer[0], 0, transferData.Length);
                    }
                    else
                    {
                        transferData.Stream = new ChunkedMemoryStream(transferData.MemoryBuffer, 0, transferData.Length);
                    }

                    Utils.SetAttributes(this.blockBlob, this.SharedTransferData.Attributes);

                    await this.Controller.SetCustomAttributesAsync(this.SharedTransferData.TransferJob.Source.Instance, this.blockBlob);

                    await this.DoUploadAndSetBlobAttributes(transferData.Stream);
                }

                // Skip transfer window calculation and related journal recording operations when it's file with only one chunk.
                if (this.EnableOneChunkFileOptimization)
                {
                    this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                }
                else
                {
                    this.Controller.UpdateProgress(() =>
                    {
                        lock (this.SharedTransferData.TransferJob.CheckPoint.TransferWindowLock)
                        {
                            this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Remove(transferData.StartOffset);
                        }

                        this.SharedTransferData.TransferJob.Transfer.UpdateJournal();

                        // update progress
                        this.Controller.UpdateProgressAddBytesTransferred(transferData.Length);
                    });
                }

                this.SetFinish();
            }
        }

        /// <summary>
        /// Upload using put blob and set customized blob attributes.
        /// Note to ensure DMLib's behavior consistency, this method: 
        /// 1. Uses x-ms-blob-content-encoding and x-ms-blob-content-language as a workaround to bypass request canonicalization
        ///    forced by REST API PutBlob.
        /// 2. As .Net core version's XSCL checks format of Cache-Control and Content-Type, uses x-ms-blob-cache-control and x-ms-blob-content-type
        ///    as a workaround to bypass XSCL's header validation when necessary.
        /// 3. As REST API PutBlob would generate ContentMD5 and overwrite customized ContentMD5, 
        ///    uses SetProperties to set customized ContentMD5 when necessary.
        /// 4. REST API PutBlob would set Content-Type to application/octet-stream by default, if provided Content-Type is null or empty.
        ///    To set Content-Type correctly, REST API SetBlobProperties must be called explicitly.
        /// </summary>
        /// <param name="sourceStream">Source stream.</param>
        /// <returns><see cref="Task"/></returns>
        private async Task DoUploadAndSetBlobAttributes(Stream sourceStream)
        {
            string providedMD5 = this.blockBlob.Properties.ContentMD5;

            var accessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition);
            var blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            var operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);
            operationContext.UserHeaders = new Dictionary<string, string>(capacity: 7); // Use 7 as capacity, a prime larger than 4, in case of collision, and runtime reallocation.

            if (!string.IsNullOrEmpty(this.blockBlob.Properties.CacheControl))
            {
                operationContext.UserHeaders.Add(
                    Shared.Protocol.Constants.HeaderConstants.BlobCacheControlHeader,
                    this.blockBlob.Properties.CacheControl);

                this.blockBlob.Properties.CacheControl = null;
            }

            if (!string.IsNullOrEmpty(this.blockBlob.Properties.ContentEncoding))
            {
                operationContext.UserHeaders.Add(
                    Shared.Protocol.Constants.HeaderConstants.BlobContentEncodingHeader,
                    this.blockBlob.Properties.ContentEncoding);

                this.blockBlob.Properties.ContentEncoding = null;
            }

            if (!string.IsNullOrEmpty(this.blockBlob.Properties.ContentLanguage))
            {
                operationContext.UserHeaders.Add(
                    Shared.Protocol.Constants.HeaderConstants.BlobContentLanguageHeader,
                    this.blockBlob.Properties.ContentLanguage);

                this.blockBlob.Properties.ContentLanguage = null;
            }

            if (!string.IsNullOrEmpty(this.blockBlob.Properties.ContentType))
            {
                operationContext.UserHeaders.Add(
                    Shared.Protocol.Constants.HeaderConstants.BlobContentTypeHeader,
                    this.blockBlob.Properties.ContentType);

                this.blockBlob.Properties.ContentType = null;
            }

            await this.blockBlob.UploadFromStreamAsync(
                sourceStream,
                accessCondition,
                blobRequestOptions,
                operationContext,
                this.CancellationToken);

            if (providedMD5 != this.blockBlob.Properties.ContentMD5
                || (this.SharedTransferData.Attributes.OverWriteAll && string.IsNullOrEmpty(this.blockBlob.Properties.ContentType))
                || (!this.SharedTransferData.Attributes.OverWriteAll && this.blockBlob.Properties.ContentType == string.Empty))
            {
                this.blockBlob.Properties.ContentMD5 = providedMD5;

                if (operationContext.UserHeaders.ContainsKey(Shared.Protocol.Constants.HeaderConstants.BlobCacheControlHeader))
                {
                    this.blockBlob.Properties.CacheControl =
                        operationContext.UserHeaders[Shared.Protocol.Constants.HeaderConstants.BlobCacheControlHeader];
                }

                if (operationContext.UserHeaders.ContainsKey(Shared.Protocol.Constants.HeaderConstants.BlobContentEncodingHeader))
                {
                    this.blockBlob.Properties.ContentEncoding =
                        operationContext.UserHeaders[Shared.Protocol.Constants.HeaderConstants.BlobContentEncodingHeader];
                }

                if (operationContext.UserHeaders.ContainsKey(Shared.Protocol.Constants.HeaderConstants.BlobContentLanguageHeader))
                {
                    this.blockBlob.Properties.ContentLanguage =
                        operationContext.UserHeaders[
                            Shared.Protocol.Constants.HeaderConstants.BlobContentLanguageHeader];
                }

                if (operationContext.UserHeaders.ContainsKey(Shared.Protocol.Constants.HeaderConstants.BlobContentTypeHeader))
                {
                    this.blockBlob.Properties.ContentType =
                        operationContext.UserHeaders[Shared.Protocol.Constants.HeaderConstants.BlobContentTypeHeader];
                }

                await this.blockBlob.SetPropertiesAsync(
                    accessCondition,
                    blobRequestOptions,
                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                    this.CancellationToken);
            }
        }

        private void SetFinish()
        {
            this.state = State.Finished;
            this.NotifyFinished(null);
            this.workToken = 0;
        }

        private void FinishBlock()
        {
            Debug.Assert(
                this.state == State.UploadBlob || this.state == State.Error,
                "FinishBlock called, but state isn't Upload or Error",
                "Current state is {0}",
                this.state);

            // If a parallel operation caused the controller to be placed in
            // error state exit, make sure not to accidentally change it to
            // the Commit state.
            if (this.state == State.Error)
            {
                return;
            }

            if (Interlocked.Read(ref this.uploadedLength) == this.SharedTransferData.TotalLength)
            {
                this.state = State.Commit;
            }
        }

        private void InitializeBlockIds()
        {
            int count = (int)Math.Ceiling((double)this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset / this.SharedTransferData.BlockSize);

            for (int i = 0; i < count; ++i)
            {
                GetBlockIdByIndex(i);
            }
        }

        private string GetBlockId(long startOffset)
        {
            Debug.Assert(startOffset % this.SharedTransferData.BlockSize == 0, "Block startOffset should be multiples of block size.");

            int index = (int)(startOffset / this.SharedTransferData.BlockSize);

            string blockId = string.Empty;

            lock (blockIdsLock)
            {
                if (this.blockIds.TryGetValue(index, out blockId))
                {
                    return blockId;
                }
            }

            return GetBlockIdByIndex(index);
        }

        private string GetBlockIdByIndex(int index)
        {
            string blockIdSuffix = index.ToString("D6", CultureInfo.InvariantCulture);
            byte[] blockIdInBytes = System.Text.Encoding.UTF8.GetBytes(this.destLocation.BlockIdPrefix + blockIdSuffix);
            string blockId = Convert.ToBase64String(blockIdInBytes);

            lock (blockIdsLock)
            {
                this.blockIds.Add(index, blockId);
            }

            return blockId;
        }
    }
}
