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

        protected override async Task<StorageCopyState> DoStartCopyAsync()
        {
            // To copy from source to blob, DataMovement Library should overwrite destination's properties and meta datas.
            // Clear destination's meta data here to avoid using destination's meta data.
            // Please reference to https://docs.microsoft.com/en-us/rest/api/storageservices/copy-file.
            this.destFile.Metadata.Clear();

            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);
            if (null != this.SourceUri)
            {
                await this.destFile.StartCopyAsync(
                    this.SourceUri,
                    null,
                    null,
                    default(FileCopyOptions),
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken); 
                
                return new StorageCopyState(this.destFile.CopyState);
            }
            else if (null != this.SourceBlob)
            {
                await this.destFile.StartCopyAsync(
                    this.SourceBlob.GenerateCopySourceUri(),
                    null,
                    null,
                    default(FileCopyOptions),
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken); 
                
                var copyState = new StorageCopyState(this.destFile.CopyState);

                if (copyState.Status == StorageCopyStatus.Success)
                {
                    copyState.TotalBytes = this.SourceBlob.Properties.Length;
                    copyState.BytesCopied = this.SourceBlob.Properties.Length;
                }
                return copyState;
            }
            else
            {
                var transfer = this.TransferJob.Transfer;
                FileCopyOptions fileCopyOptions = new FileCopyOptions();

                if (transfer.PreserveSMBAttributes)
                {
                    fileCopyOptions.PreserveCreationTime = transfer.PreserveSMBAttributes;
                    fileCopyOptions.PreserveLastWriteTime = transfer.PreserveSMBAttributes;
                    fileCopyOptions.PreserveNtfsAttributes = transfer.PreserveSMBAttributes;
                    fileCopyOptions.SetArchive = false;
                }

                fileCopyOptions.PreservePermissions = (transfer.PreserveSMBPermissions != PreserveSMBPermissions.None);

                await this.destFile.StartCopyAsync(
                    this.SourceFile.GenerateCopySourceUri(fileCopyOptions.PreservePermissions),
                    null,
                    null,
                    fileCopyOptions,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken); 
                
                var copyState = new StorageCopyState(this.destFile.CopyState);

                if (copyState.Status == StorageCopyStatus.Success)
                {
                    copyState.TotalBytes = this.SourceFile.Properties.Length;
                    copyState.BytesCopied = this.SourceFile.Properties.Length;
                }
                return copyState;
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

            await setCustomAttributes(this.TransferJob.Source.Instance, this.destFile);

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
