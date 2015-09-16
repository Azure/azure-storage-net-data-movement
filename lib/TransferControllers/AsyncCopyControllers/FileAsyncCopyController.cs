//------------------------------------------------------------------------------
// <copyright file="FileAsyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Azure file asynchronous copy.
    /// </summary>
    internal class FileAsyncCopyController : AsyncCopyController
    {
        private CloudFile destFile;

        public FileAsyncCopyController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
            : base(transferScheduler, transferJob, cancellationToken)
        {
            if (null == transferJob.Destination.AzureFile)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "Dest.AzureFile"),
                    "transferJob");
            }

            if ((null == transferJob.Source.SourceUri && null == transferJob.Source.Blob && null == transferJob.Source.AzureFile)
                || (null != transferJob.Source.SourceUri && null != transferJob.Source.Blob)
                || (null != transferJob.Source.Blob && null != transferJob.Source.AzureFile)
                || (null != transferJob.Source.SourceUri && null != transferJob.Source.AzureFile))
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ProvideExactlyOneOfThreeParameters,
                        "Source.SourceUri",
                        "Source.Blob",
                        "Source.AzureFile"),
                    "transferJob");
            }

            this.destFile = this.TransferJob.Destination.AzureFile;
        }

        protected override Uri DestUri
        {
            get
            {
                return this.destFile.Uri;
            }
        }

        protected override Task DoFetchDestAttributesAsync()
        {
            return this.destFile.FetchAttributesAsync(
                null,
                Utils.GenerateFileRequestOptions(this.TransferJob.Destination.FileRequestOptions),
                null,
                this.CancellationToken);
        }

        protected override Task<string> DoStartCopyAsync()
        {
            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);
            if (null != this.SourceUri)
            {
                return this.destFile.StartCopyAsync(
                    this.SourceUri,
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.TransferJob.Destination.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
            else if (null != this.SourceBlob)
            {
                return this.destFile.StartCopyAsync(
                    this.SourceBlob.GenerateCopySourceBlob(),
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.TransferJob.Destination.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
            else
            {
                return this.destFile.StartCopyAsync(
                    this.SourceFile.GenerateCopySourceFile(),
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.TransferJob.Destination.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
        }

        protected override void DoHandleGetDestinationException(StorageException se)
        {
        }

        protected override async Task<CopyState> FetchCopyStateAsync()
        {
            await this.destFile.FetchAttributesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.TransferJob.Destination.AccessCondition),
                Utils.GenerateFileRequestOptions(this.TransferJob.Destination.FileRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            return this.destFile.CopyState;
        }
    }
}
