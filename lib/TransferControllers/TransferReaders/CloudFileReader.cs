//------------------------------------------------------------------------------
// <copyright file="CloudFileReader.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.File;

    class CloudFileReader : RangeBasedReader
    {
        private AzureFileLocation sourceLocation;
        private CloudFile cloudFile;

        public CloudFileReader(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            :base(scheduler, controller, cancellationToken)
        {
            this.sourceLocation = this.SharedTransferData.TransferJob.Source as AzureFileLocation;
            this.cloudFile = this.sourceLocation.AzureFile;
            Debug.Assert(null != this.cloudFile, "Initializing a CloudFileReader, the source location should be a CloudFile instance.");
        }

        protected override async Task DoFetchAttributesAsync()
        {         
            await this.cloudFile.FetchAttributesAsync(
                null,
                Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if ((0 != this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset)
                    || (this.SharedTransferData.TransferJob.CheckPoint.TransferWindow.Any()))
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceLocation.AzureFile.Properties.ETag;
            }
            else if ((this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset > this.sourceLocation.AzureFile.Properties.Length)
                || (this.SharedTransferData.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.SharedTransferData.DisableContentMD5Validation =
                null != this.sourceLocation.FileRequestOptions ?
                this.sourceLocation.FileRequestOptions.DisableContentMD5Validation.HasValue ?
                this.sourceLocation.FileRequestOptions.DisableContentMD5Validation.Value : false : false;

            this.SharedTransferData.Attributes = Utils.GenerateAttributes(this.cloudFile, this.SharedTransferData.TransferJob.Transfer.PreserveSMBAttributes);
            this.SharedTransferData.TotalLength = this.cloudFile.Properties.Length;
        }

        protected override async Task<List<Range>> DoGetRangesAsync(RangesSpan rangesSpan)
        {
            List<Range> rangeList = new List<Range>();

            foreach (var fileRange in await this.cloudFile.ListRangesAsync(
                     rangesSpan.StartOffset,
                     rangesSpan.EndOffset - rangesSpan.StartOffset + 1,
                     null,
                     Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                     Utils.GenerateOperationContext(this.Controller.TransferContext),
                     this.CancellationToken))
            {
                rangeList.Add(new Range()
                {
                    StartOffset = fileRange.StartOffset,
                    EndOffset = fileRange.EndOffset,
                    HasData = true
                });
            }

            return rangeList;
        }

        protected override async Task DoDownloadRangeToStreamAsync(RangeBasedDownloadState asyncState)
        {
            await this.sourceLocation.AzureFile.DownloadRangeToStreamAsync(
                asyncState.DownloadStream,
                asyncState.StartOffset,
                asyncState.Length,
                null,
                Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
        }
    }
}
