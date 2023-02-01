//------------------------------------------------------------------------------
// <copyright file="PageBlobWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal sealed class PageBlobWriter : RangeBasedWriter
    {
        private AzureBlobLocation destLocation;
        private CloudPageBlob pageBlob;

        /// <summary>
        /// Size of all files transferred to page blob must be exactly 
        /// divided by this constant.
        /// </summary>
        private const long PageBlobPageSize = (long)512;

        /// <summary>
        /// To indicate whether the destination already exist before this writing.
        /// If no, when try to set destination's attribute, should get its attributes first.
        /// </summary>
        private bool destExist = false;

        internal PageBlobWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.TransferJob.Destination as AzureBlobLocation;
            this.pageBlob = this.destLocation.Blob as CloudPageBlob;
        }

        protected override Uri DestUri
        {
            get 
            {
                return this.pageBlob.Uri;
            }
        }

        protected override void CheckInputStreamLength(long inputStreamLength)
        {
            if (inputStreamLength < 0)
            {
                throw new TransferException(
                    TransferErrorCode.UploadBlobSourceFileSizeInvalid,
                    string.Format(CultureInfo.CurrentCulture, Resources.SourceMustBeFixedSize, Resources.PageBlob));
            }

            if (0 != inputStreamLength % PageBlobPageSize)
            {
                string exceptionMessage = string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.BlobFileSizeInvalidException,
                    Utils.BytesToHumanReadableSize(inputStreamLength),
                    Resources.PageBlob,
                    Utils.BytesToHumanReadableSize(PageBlobPageSize));

                throw new TransferException(
                    TransferErrorCode.UploadBlobSourceFileSizeInvalid,
                    exceptionMessage);
            }

            return;
        }

        protected override async Task DoFetchAttributesAsync()
        {
            await this.pageBlob.FetchAttributesAsync(
                this.destLocation.AccessCondition,
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
            this.destExist = true;
        }

        protected override void HandleFetchAttributesResult(Exception e)
        {
            StorageException se = e as StorageException;
            if (null != se &&
                (0 == string.Compare(se.Message, Constants.BlobTypeMismatch, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch, se);
            }
        }

        protected override async Task DoCreateAsync(long size)
        {
            if (this.destExist)
            {
                this.CleanupPropertyForCanonicalization();
            }

            await this.pageBlob.CreateAsync(
                size,
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions, true),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken);
        }

        protected override async Task WriteRangeAsync(TransferData transferData)
        {
            await this.pageBlob.WritePagesAsync(
                transferData.Stream,
                transferData.StartOffset,
                null,
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken).ConfigureAwait(false);
        }

        protected override async Task DoCommitAsync()
        {
            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            if (!this.Controller.IsForceOverwrite && !this.destExist)
            {
                await this.pageBlob.FetchAttributesAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.pageBlob.Metadata);
            Utils.SetAttributes(this.pageBlob, this.SharedTransferData.Attributes);
            await this.Controller.SetCustomAttributesAsync(this.TransferJob.Source.Instance, this.pageBlob);

            await this.pageBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                null,
                this.CancellationToken);
            
            if (!originalMetadata.DictionaryEquals(this.pageBlob.Metadata))
            {
                await this.pageBlob.SetMetadataAsync(
                             Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                             blobRequestOptions,
                             operationContext,
                             this.CancellationToken);
            }
        }

        /// <summary>
        /// Cleanup properties that might cause request canonicalization check failure.
        /// </summary>
        private void CleanupPropertyForCanonicalization()
        {
            this.pageBlob.Properties.ContentLanguage = null;
            this.pageBlob.Properties.ContentEncoding = null;
        }
    }
}
