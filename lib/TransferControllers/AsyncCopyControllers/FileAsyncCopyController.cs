//------------------------------------------------------------------------------
// <copyright file="FileAsyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.DataMovement.Extensions;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Azure file asynchronous copy.
    /// </summary>
    internal class FileAsyncCopyController : AsyncCopyController
    {
        private AzureFileLocation destLocation;
        private CloudFile destFile;

        public FileAsyncCopyController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
            : base(transferScheduler, transferJob, cancellationToken)
        {
            if (transferJob.Destination.Type != TransferLocationType.AzureFile)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "Dest.AzureFile"),
                    "transferJob");
            }

            if (transferJob.Source.Type != TransferLocationType.SourceUri &&
                transferJob.Source.Type != TransferLocationType.AzureBlob &&
                transferJob.Source.Type != TransferLocationType.AzureFile)
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

            this.destLocation = this.TransferJob.Destination as AzureFileLocation;
            this.destFile = this.destLocation.AzureFile;
        }

        protected override Uri DestUri
        {
            get
            {
                return this.destFile.SnapshotQualifiedUri;
            }
        }

        protected override Task DoFetchDestAttributesAsync()
        {
            return this.destFile.FetchAttributesAsync(
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                null,
                this.CancellationToken);
        }

        protected override Task<string> DoStartCopyAsync()
        {
            // To copy from source to blob, DataMovement Library should overwrite destination's properties and meta datas.
            // Clear destination's meta data here to avoid using destination's meta data.
            // Please reference to https://docs.microsoft.com/en-us/rest/api/storageservices/copy-file.
            this.destFile.Metadata.Clear();

            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);
            if (null != this.SourceUri)
            {
                return this.destFile.StartCopyAsync(
                    this.SourceUri,
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
            else if (null != this.SourceBlob)
            {
                return this.destFile.StartCopyAsync(
                    this.SourceBlob.GenerateCopySourceUri(),
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
            else
            {
                return this.destFile.StartCopyAsync(
                    this.SourceFile.GenerateCopySourceUri(),
                    null,
                    null,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
        }

        protected override void DoHandleGetDestinationException(StorageException se)
        {
        }

        protected override async Task<StorageCopyState> FetchCopyStateAsync()
        {
            await this.destFile.FetchAttributesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            return new StorageCopyState(this.destFile.CopyState);
        }

        protected override async Task SetAttributesAsync(SetAttributesCallbackAsync setCustomAttributes)
        {
            var originalAttributes = Utils.GenerateAttributes(this.destFile, true);
            var originalMetadata = new Dictionary<string, string>(this.destFile.Metadata);

            await setCustomAttributes(this.destFile);

            if (!Utils.CompareProperties(originalAttributes, Utils.GenerateAttributes(this.destFile, true)))
            {
                await this.destFile.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);
            }

            if (!originalMetadata.DictionaryEquals(this.destFile.Metadata))
            {
                await this.destFile.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }
        }
    }
}
