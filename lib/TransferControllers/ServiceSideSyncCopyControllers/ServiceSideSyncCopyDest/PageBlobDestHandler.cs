//------------------------------------------------------------------------------
// <copyright file="PageBlobDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal class PageBlobDestHandler : BlobDestHandler
    {
        private CloudPageBlob destPageBlob;

        public PageBlobDestHandler(AzureBlobLocation destLocation, TransferJob transferJob)
            : base(destLocation, transferJob)
        {
            this.destPageBlob = destLocation.Blob as CloudPageBlob;
        }

        protected override Task CreateDestinationAsync(long totalLength, AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return this.destPageBlob.CreateAsync(
                    totalLength,
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.DestLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    cancellationToken);
        }
    }
}
