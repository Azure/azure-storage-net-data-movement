//------------------------------------------------------------------------------
// <copyright file="PageBlobReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal sealed class PageBlobReader : RangeBasedReader
    {
        private CloudPageBlob pageBlob;

        public PageBlobReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            :base(scheduler, controller, cancellationToken)
        {
            pageBlob = this.SharedTransferData.TransferJob.Source.Blob as CloudPageBlob;
            Debug.Assert(null != this.pageBlob, "Initializing a PageBlobReader, the source location should be a CloudPageBlob instance.");
        }

        protected override async Task DoFetchAttributesAsync()
        {
            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.Location.ETag,
                this.Location.AccessCondition,
                this.Location.CheckedAccessCondition);

            await this.pageBlob.FetchAttributesAsync(
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.Location.BlobRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);

            if (string.IsNullOrEmpty(this.Location.ETag))
            {
                if ((0 != this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                    || (this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Any()))
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.Location.ETag = this.Location.Blob.Properties.ETag;
            }
            else if ((this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset > this.Location.Blob.Properties.Length)
                || (this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.SharedTransferData.DisableContentMD5Validation =
                null != this.Location.BlobRequestOptions ?
                this.Location.BlobRequestOptions.DisableContentMD5Validation.HasValue ?
                this.Location.BlobRequestOptions.DisableContentMD5Validation.Value : false : false;

            this.SharedTransferData.Attributes = Utils.GenerateAttributes(this.pageBlob);
            this.SharedTransferData.TotalLength = this.pageBlob.Properties.Length;
            this.SharedTransferData.SourceLocation = this.pageBlob.Uri.ToString();
        }

        protected override async Task<List<Range>> DoGetRangesAsync(RangesSpan rangesSpan)
        {
            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.Location.Blob.Properties.ETag,
                this.Location.AccessCondition);

            List<Range> rangeList = new List<Range>();

            foreach (var pageRange in await this.pageBlob.GetPageRangesAsync(
                    rangesSpan.StartOffset,
                    rangesSpan.EndOffset - rangesSpan.StartOffset + 1,
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(this.Location.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.Controller.TransferContext),
                    this.CancellationToken))
            {
                rangeList.Add(new Range() 
                {
                    StartOffset = pageRange.StartOffset,
                    EndOffset = pageRange.EndOffset,
                    HasData = true
                });
            }

            return rangeList;
        }

        protected override async Task DoDownloadRangeToStreamAsync(RangeBasedDownloadState asyncState)
        {
            AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                this.Location.Blob.Properties.ETag,
                this.Location.AccessCondition);

            await this.Location.Blob.DownloadRangeToStreamAsync(
                asyncState.DownloadStream,
                asyncState.StartOffset,
                asyncState.Length,
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.Location.BlobRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
        }
    }
}
