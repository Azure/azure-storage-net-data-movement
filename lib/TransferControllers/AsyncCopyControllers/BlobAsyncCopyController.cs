//------------------------------------------------------------------------------
// <copyright file="BlobAsyncCopyController.cs" company="Microsoft">
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

    /// <summary>
    /// Blob asynchronous copy.
    /// </summary>
    internal class BlobAsyncCopyController : AsyncCopyController
    {
        private AzureBlobLocation destLocation;
        private CloudBlob destBlob;

        public BlobAsyncCopyController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
            : base(transferScheduler, transferJob, cancellationToken)
        {
            this.destLocation = transferJob.Destination as AzureBlobLocation;
            CloudBlob transferDestBlob = this.destLocation.Blob;
            if (null == transferDestBlob)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "Dest.Blob"),
                    "transferJob");
            }

            if (transferDestBlob.IsSnapshot)
            {
                throw new ArgumentException(Resources.DestinationMustBeBaseBlob, "transferJob");
            }

            AzureBlobLocation sourceBlobLocation = transferJob.Source as AzureBlobLocation;
            if (sourceBlobLocation != null)
            {
                if (sourceBlobLocation.Blob.BlobType != transferDestBlob.BlobType)
                {
                    throw new ArgumentException(Resources.SourceAndDestinationBlobTypeDifferent, "transferJob");
                }

                if (StorageExtensions.Equals(sourceBlobLocation.Blob, transferDestBlob))
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
                }
            }

            this.destBlob = transferDestBlob;
        }

        protected override Uri DestUri
        {
            get
            {
                return this.destBlob.Uri;
            }
        }

        protected override Task DoFetchDestAttributesAsync()
        {
            AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                this.destLocation.AccessCondition,
                this.destLocation.CheckedAccessCondition);

            return this.destBlob.FetchAttributesAsync(
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);
        }

        protected override async Task<StorageCopyState> DoStartCopyAsync()
        {
            AccessCondition destAccessCondition = Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition);

            // To copy from source to blob, DataMovement Library should overwrite destination's properties and meta datas.
            // Clear destination's meta data here to avoid using destination's meta data.
            // Please reference to https://docs.microsoft.com/en-us/rest/api/storageservices/Copy-Blob.
            this.destBlob.Metadata.Clear();

            if (null != this.SourceUri)
            {
                await this.destBlob.StartCopyAsync(
                    this.SourceUri,
                    null,
                    destAccessCondition,
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);

                return new StorageCopyState(this.destBlob.CopyState);
            }
            else if (null != this.SourceBlob)
            {
                AccessCondition sourceAccessCondition =
                    AccessCondition.GenerateIfMatchCondition(this.SourceBlob.Properties.ETag);

                await this.destBlob.StartCopyAsync(
                         this.SourceBlob.GenerateCopySourceUri(),
                         sourceAccessCondition,
                         destAccessCondition,
                         Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                         Utils.GenerateOperationContext(this.TransferContext),
                         this.CancellationToken);

                var copyState = new StorageCopyState(this.destBlob.CopyState);

                if (copyState.Status == StorageCopyStatus.Success)
                {
                    copyState.TotalBytes = this.SourceBlob.Properties.Length;
                    copyState.BytesCopied = this.SourceBlob.Properties.Length;
                }
                return copyState;
            }
            else
            {
                if (BlobType.BlockBlob == this.destBlob.BlobType)
                {
                    await (this.destBlob as CloudBlockBlob).StartCopyAsync(
                             this.SourceFile.GenerateCopySourceUri(),
                             null,
                             destAccessCondition,
                             Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                             Utils.GenerateOperationContext(this.TransferContext),
                             this.CancellationToken);

                    var copyState = new StorageCopyState(this.destBlob.CopyState);

                    if (copyState.Status == StorageCopyStatus.Success)
                    {
                        copyState.TotalBytes = this.SourceFile.Properties.Length;
                        copyState.BytesCopied = this.SourceFile.Properties.Length;
                    }
                    return copyState;
                }
                else if (BlobType.PageBlob == this.destBlob.BlobType)
                {
                    throw new InvalidOperationException(Resources.AsyncCopyFromFileToPageBlobNotSupportException);
                }
                else if (BlobType.AppendBlob == this.destBlob.BlobType)
                {
                    throw new InvalidOperationException(Resources.AsyncCopyFromFileToAppendBlobNotSupportException);
                }
                else
                {
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.NotSupportedBlobType,
                        this.destBlob.BlobType));
                }
            }
        }

        protected override void DoHandleGetDestinationException(StorageException se)
        {
            if (null != se)
            {
                if (0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase))
                {
                    // Current use error message to decide whether it caused by blob type mismatch,
                    // We should ask xscl to expose an error code for this..
                    // Opened workitem 1487579 to track this.
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
                }
            }
            else
            {
                if (null != this.SourceBlob && this.SourceBlob.Properties.BlobType != this.destBlob.Properties.BlobType)
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationBlobTypeDifferent);
                }
            }
        }

        protected override async Task<StorageCopyState> FetchCopyStateAsync()
        {
            await this.destBlob.FetchAttributesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            return new StorageCopyState(this.destBlob.CopyState);
        }

        protected override async Task SetAttributesAsync(SetAttributesCallbackAsync setCustomAttributes)
        {
            var originalAttributes = Utils.GenerateAttributes(this.destBlob);
            var originalMetadata = new Dictionary<string, string>(this.destBlob.Metadata);

            await setCustomAttributes(this.TransferJob.Source.Instance, this.destBlob);

            if (!Utils.CompareProperties(originalAttributes, Utils.GenerateAttributes(this.destBlob)))
            {
                await this.destBlob.SetPropertiesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }

            if (!originalMetadata.DictionaryEquals(this.destBlob.Metadata))
            {
                await this.destBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }
        }
    }
}
