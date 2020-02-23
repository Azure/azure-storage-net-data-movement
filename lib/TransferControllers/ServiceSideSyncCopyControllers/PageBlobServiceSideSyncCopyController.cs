//------------------------------------------------------------------------------
// <copyright file="PageBlobServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Transfer controller to copy to page blob with PutPageFromURL.
    /// </summary>
    class PageBlobServiceSideSyncCopyController : RangeBasedServiceSideSyncCopy
    {
        private CloudPageBlob destPageBlob;
        private AzureBlobLocation destLocation;

        /// <summary>
        /// Initializes a new instance of the <see cref="PageBlobServiceSideSyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal PageBlobServiceSideSyncCopyController(
            TransferScheduler scheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(scheduler, transferJob, userCancellationToken)
        {
            TransferLocation sourceLocation = transferJob.Source;

            if (sourceLocation.Type == TransferLocationType.AzureBlob)
            {
                var blobLocation = sourceLocation as AzureBlobLocation;
                var pageBlob = blobLocation.Blob as CloudPageBlob;

                if (null != pageBlob)
                {
                    this.SourceHandler = new ServiceSideSyncCopySource.PageBlobSourceHandler(blobLocation, transferJob);
                }
                else
                {
                    this.SourceHandler = new ServiceSideSyncCopySource.BlobSourceHandler(blobLocation, transferJob);
                }
            }
            else if (sourceLocation.Type == TransferLocationType.AzureFile)
            {
                this.SourceHandler = new ServiceSideSyncCopySource.FileSourceHandler(sourceLocation as AzureFileLocation, transferJob);
            }
            else
            {
                throw new ArgumentException(
                    Resources.OnlySupportBlobAzureFileSource,
                    "transferJob");
            }

            this.destLocation = transferJob.Destination as AzureBlobLocation;
            this.destPageBlob = this.destLocation.Blob as CloudPageBlob;
            this.DestHandler = new ServiceSideSyncCopyDest.PageBlobDestHandler(this.destLocation, transferJob);
            this.hasWork = true;
        }

        protected override Task CopyChunkFromUriAsync(long startOffset, long length)
        {
            var sourceAccessCondition = this.SourceHandler.NeedToCheckAccessCondition 
                ? Utils.GenerateIfMatchConditionWithCustomerCondition(this.SourceHandler.ETag, this.SourceHandler.AccessCondition, true)
                : null;

            return this.destPageBlob.WritePagesAsync(
                this.SourceHandler.GetCopySourceUri(), 
                startOffset, // The byte offset in the source at which to begin retrieving content.
                length, //The number of bytes from the source to copy
                startOffset, //  The offset in destination at which to begin writing, in bytes.
                null,
                sourceAccessCondition,
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);
        }
    }
}
