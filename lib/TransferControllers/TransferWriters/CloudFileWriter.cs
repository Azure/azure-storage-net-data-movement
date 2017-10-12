//------------------------------------------------------------------------------
// <copyright file="CloudFileWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.File;

    internal sealed class CloudFileWriter : RangeBasedWriter
    {
        private AzureFileLocation destLocation;
        private CloudFile cloudFile;

        /// <summary>
        /// To indicate whether the destination already exist before this writing.
        /// If no, when try to set destination's attribute, should get its attributes first.
        /// </summary>
        private bool destExist = false;

        internal CloudFileWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.TransferJob.Destination as AzureFileLocation;
            this.cloudFile = this.destLocation.AzureFile;
        }

        protected override Uri DestUri
        {
            get 
            {
                return this.cloudFile.SnapshotQualifiedUri;
            }
        }

        protected override void CheckInputStreamLength(long inputStreamLength)
        {
            if (inputStreamLength > Constants.MaxCloudFileSize)
            {
                string exceptionMessage = string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.CloudFileSizeTooLargeException,
                    Utils.BytesToHumanReadableSize(inputStreamLength),
                    Utils.BytesToHumanReadableSize(Constants.MaxCloudFileSize));

                throw new TransferException(
                    TransferErrorCode.UploadSourceFileSizeTooLarge,
                    exceptionMessage);
            }

            return;
        }

        protected override async Task DoFetchAttributesAsync()
        {
            await this.cloudFile.FetchAttributesAsync(
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
            this.destExist = true;
        }

        protected override void HandleFetchAttributesResult(Exception e)
        {
            // Do nothing here.
        }

        protected override async Task DoCreateAsync(long size)
        {
            await this.cloudFile.CreateAsync(
                size,
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions, true),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
        }

        protected override async Task WriteRangeAsync(TransferData transferData)
        {
            await this.cloudFile.WriteRangeAsync(
                transferData.Stream,
                transferData.StartOffset,
                null,
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
        }

        protected override async Task DoCommitAsync()
        {
            FileRequestOptions fileRequestOptions = Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            if (!this.Controller.IsForceOverwrite && !this.destExist)
            {
                await this.cloudFile.FetchAttributesAsync(
                    null,
                    fileRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.cloudFile.Metadata);
            Utils.SetAttributes(this.cloudFile, this.SharedTransferData.Attributes);
            await this.Controller.SetCustomAttributesAsync(this.cloudFile);

            await this.cloudFile.SetPropertiesAsync(
                null,
                fileRequestOptions,
                operationContext,
                this.CancellationToken);

            if (!originalMetadata.DictionaryEquals(this.cloudFile.Metadata))
            {
                await this.cloudFile.SetMetadataAsync(
                    null,
                    fileRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }
        }
    }
}
