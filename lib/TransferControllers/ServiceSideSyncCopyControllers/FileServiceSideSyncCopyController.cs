//------------------------------------------------------------------------------
// <copyright file="FileServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;

    class FileServiceSideSyncCopyController : RangeBasedServiceSideSyncCopy
    {
        private CloudFile destFile;
        private AzureFileLocation destLocation;

        /// <summary>
        /// Initializes a new instance of the <see cref="PageBlobServiceSideSyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal FileServiceSideSyncCopyController(
            TransferScheduler scheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(scheduler, transferJob, userCancellationToken)
        {
            TransferLocation sourceLocation = transferJob.Source;

            if (sourceLocation.Type == TransferLocationType.AzureBlob)
            {
                var blobLocation = sourceLocation as AzureBlobLocation;

                if (blobLocation.Blob is CloudPageBlob)
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

            this.destLocation = transferJob.Destination as AzureFileLocation;
            this.destFile = this.destLocation.AzureFile as CloudFile;
            this.DestHandler = new ServiceSideSyncCopyDest.FileDestHandler(this.destLocation, transferJob);
            this.hasWork = true;
        }

        protected override Task CopyChunkFromUriAsync(long startOffset, long length)
        {
            return this.destFile.WriteRangeAsync(
                this.SourceHandler.GetCopySourceUri(),
                startOffset, // The byte offset in the source at which to begin retrieving content.
                length, //The number of bytes from the source to copy
                startOffset, //  The offset in destination at which to begin writing, in bytes.
                null,
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);
        }
    }
}
