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
                this.sourceLocation.FileRequestOptions.DisableContentMD5Validation ?? false : false;

            var transfer = this.SharedTransferData.TransferJob.Transfer;

            this.SharedTransferData.Attributes = Utils.GenerateAttributes(this.cloudFile, transfer.PreserveSMBAttributes);

            if (PreserveSMBPermissions.None != transfer.PreserveSMBPermissions)
            {
                if (!string.IsNullOrEmpty(this.cloudFile.FilePermission))
                {
                    this.SharedTransferData.Attributes.PortableSDDL = this.cloudFile.FilePermission;
                }
                else if (!string.IsNullOrEmpty(this.cloudFile.Properties.FilePermissionKey))
                {
                    var sddlCache = this.SharedTransferData.TransferJob.Transfer.SDDLCache;

                    if (null != sddlCache)
                    {
                        string portableSDDL = null;
                        sddlCache.TryGetValue(this.cloudFile.Properties.FilePermissionKey, out portableSDDL);

                        if (null == portableSDDL)
                        {
                            portableSDDL = await this.cloudFile.Share.GetFilePermissionAsync(this.cloudFile.Properties.FilePermissionKey,
                                Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                                Utils.GenerateOperationContext(this.Controller.TransferContext),
                                this.CancellationToken).ConfigureAwait(false);

                            sddlCache.TryAddValue(this.cloudFile.Properties.FilePermissionKey, portableSDDL);
                        }

                        this.SharedTransferData.Attributes.PortableSDDL = portableSDDL;
                    }
                    else
                    {
                        this.SharedTransferData.Attributes.PortableSDDL = await this.cloudFile.Share.GetFilePermissionAsync(this.cloudFile.Properties.FilePermissionKey,
                            Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                            Utils.GenerateOperationContext(this.Controller.TransferContext),
                            this.CancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    this.SharedTransferData.Attributes.PortableSDDL = null;
                }
            }

            this.SharedTransferData.TotalLength = this.cloudFile.Properties.Length;
        }

        protected override async Task<List<Utils.Range>> DoGetRangesAsync(Utils.RangesSpan rangesSpan)
        {
            List<Utils.Range> rangeList = new List<Utils.Range>();

            foreach (var fileRange in await this.cloudFile.ListRangesAsync(
                     rangesSpan.StartOffset,
                     rangesSpan.EndOffset - rangesSpan.StartOffset + 1,
                     null,
                     Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                     Utils.GenerateOperationContext(this.Controller.TransferContext),
                     this.CancellationToken))
            {
                rangeList.Add(new Utils.Range()
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
