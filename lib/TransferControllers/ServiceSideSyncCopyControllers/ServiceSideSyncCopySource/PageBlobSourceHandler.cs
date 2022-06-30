//------------------------------------------------------------------------------
// <copyright file="PageBlobSourceHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopySource
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    class PageBlobSourceHandler : BlobSourceHandler, IRangeBasedSourceHandler
    {
        CloudPageBlob pageBlob;

        public PageBlobSourceHandler(AzureBlobLocation sourceBlobLocation, TransferJob transferJob)
            : base(sourceBlobLocation, transferJob)
        {
            this.pageBlob = sourceBlobLocation.Blob as CloudPageBlob;
        }

        public async Task<List<Utils.Range>> GetCopyRangesAsync(long startOffset, long length, CancellationToken cancellationToken)
        {
            var pageRanges = await this.pageBlob.GetPageRangesAsync(
                startOffset,
                length,
                Utils.GenerateConditionWithCustomerCondition(this.SourceLocation.AccessCondition, this.SourceLocation.CheckedAccessCondition),
                Utils.GenerateBlobRequestOptions(this.SourceLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                cancellationToken).ConfigureAwait(false);

            List<Utils.Range> ranges = new List<Utils.Range>();

            foreach (var pageRange in pageRanges)
            {
                ranges.Add(new Utils.Range()
                {
                    StartOffset = pageRange.StartOffset,
                    EndOffset = pageRange.EndOffset,
                    HasData = true
                });
            }

            return ranges;
        }
    }
}
