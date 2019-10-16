//------------------------------------------------------------------------------
// <copyright file="AppendBlobDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal class AppendBlobDestHandler : BlobDestHandler
    {
        private CloudAppendBlob destAppendBlob;

        public AppendBlobDestHandler(AzureBlobLocation destLocation, TransferJob transferJob)
            : base(destLocation, transferJob)
        {
            this.destAppendBlob = destLocation.Blob as CloudAppendBlob;
        }

        protected override Task CreateDestinationAsync(long totalLength, AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return this.destAppendBlob.CreateOrReplaceAsync(
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.DestLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.TransferContext),
                        cancellationToken);
        }
    }
}
