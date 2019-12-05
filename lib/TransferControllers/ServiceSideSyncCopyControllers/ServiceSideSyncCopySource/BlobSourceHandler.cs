//------------------------------------------------------------------------------
// <copyright file="BlobSourceHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopySource
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal class BlobSourceHandler : ISourceHandler
    {
        private AzureBlobLocation sourceLocation;
        private CloudBlob sourceBlob;
        private TransferContext transferContext;

        private Attributes sourceAttributes;
        private long totalLength;
        private TransferJob transferJob;

        public BlobSourceHandler(AzureBlobLocation sourceBlobLocation, TransferJob transferJob)
        {
            this.sourceLocation = sourceBlobLocation;
            this.transferJob = transferJob;
            this.sourceBlob = this.sourceLocation.Blob;
            this.transferContext = this.transferJob.Transfer.Context;
        }

        protected AzureBlobLocation SourceLocation
        {
            get { return this.sourceLocation; }
        }

        protected TransferContext TransferContext
        {
            get { return this.transferContext; }
        }

        public string ETag
        {
            get { return this.sourceLocation.ETag; }
        }

        public AccessCondition AccessCondition
        {
            get { return this.sourceLocation.AccessCondition; }
        }

        public Uri Uri
        {
            get { return this.sourceBlob.Uri; }
        }

        public bool NeedToCheckAccessCondition
        {
            get { return true; }
        }

        public Task DownloadRangeToStreamAsync(Stream stream,
            long startOffset,
            long length,
            AccessCondition accessCondition,
            bool useTransactionalMD5,
            OperationContext operationContext,
            CancellationToken cancellationToken)
        {
            return this.sourceBlob.DownloadRangeToStreamAsync(stream, startOffset, length, accessCondition, new BlobRequestOptions
                {
                    UseTransactionalMD5 = useTransactionalMD5
                },
                operationContext,
                cancellationToken);
        }

        public async Task FetchAttributesAsync(CancellationToken cancellationToken)
        {
            if (this.sourceLocation.IsInstanceInfoFetched != true)
            {
                AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                     this.sourceLocation.ETag,
                     this.sourceLocation.AccessCondition,
                     this.sourceLocation.CheckedAccessCondition);
                try
                {
                    await this.sourceBlob.FetchAttributesAsync(
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.transferContext),
                        cancellationToken);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
                catch (StorageException e)
                {
#endif
                    HandleFetchSourceAttributesException(e);
                    throw;
                }

                this.sourceLocation.CheckedAccessCondition = true;
            }

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if (0 != this.transferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceBlob.Properties.ETag;
            }
            else if ((this.transferJob.CheckPoint.EntryTransferOffset > this.sourceBlob.Properties.Length)
                 || (this.transferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.sourceAttributes = Utils.GenerateAttributes(this.sourceBlob);

            this.totalLength = this.sourceBlob.Properties.Length;
        }

        private static void HandleFetchSourceAttributesException(StorageException e)
        {
            // Getting a storage exception is expected if the source doesn't
            // exist. For those cases that indicate the source doesn't exist
            // we will set a specific error state.
            if (e?.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException(Resources.SourceDoesNotExistException, e);
            }
        }

        public Uri GetCopySourceUri()
        {
            return this.sourceBlob.GenerateCopySourceUri();
        }

        public Attributes SourceAttributes
        {
            get
            {
                return this.sourceAttributes;
            }
        }

        public long TotalLength
        {
            get
            {
                return this.totalLength;
            }
        }

    }
}
