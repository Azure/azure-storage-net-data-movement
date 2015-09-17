//------------------------------------------------------------------------------
// <copyright file="BlobAsyncCopyController.cs" company="Microsoft">
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

    /// <summary>
    /// Blob asynchronous copy.
    /// </summary>
    internal class BlobAsyncCopyController : AsyncCopyController
    {
        private CloudBlob destBlob;

        public BlobAsyncCopyController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken cancellationToken)
            : base(transferScheduler, transferJob, cancellationToken)
        {
            CloudBlob transferDestBlob = transferJob.Destination.Blob;
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

            CloudBlob transferSourceBlob = transferJob.Source.Blob;

            if (null != transferSourceBlob && transferDestBlob.BlobType != transferSourceBlob.BlobType)
            {
                throw new ArgumentException(Resources.SourceAndDestinationBlobTypeDifferent, "transferJob");
            }

            if ((null != transferSourceBlob)
                && (StorageExtensions.Equals(transferSourceBlob, transferDestBlob)))
            {
                throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
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
                this.TransferJob.Destination.AccessCondition,
                this.TransferJob.Destination.CheckedAccessCondition);

            return this.destBlob.FetchAttributesAsync(
                accessCondition,
                Utils.GenerateBlobRequestOptions(this.TransferJob.Destination.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);
        }

        protected override Task<string> DoStartCopyAsync()
        {
            AccessCondition destAccessCondition = Utils.GenerateConditionWithCustomerCondition(this.TransferJob.Destination.AccessCondition);

            if (null != this.SourceUri)
            {
                return this.destBlob.StartCopyAsync(
                    this.SourceUri,
                    null,
                    destAccessCondition,
                    Utils.GenerateBlobRequestOptions(this.TransferJob.Destination.BlobRequestOptions),
                    Utils.GenerateOperationContext(this.TransferContext),
                    this.CancellationToken);
            }
            else if (null != this.SourceBlob)
            {
                AccessCondition sourceAccessCondition =
                    AccessCondition.GenerateIfMatchCondition(this.SourceBlob.Properties.ETag);

                return this.destBlob.StartCopyAsync(
                         this.SourceBlob.GenerateUriWithCredentials(),
                         sourceAccessCondition,
                         destAccessCondition,
                         Utils.GenerateBlobRequestOptions(this.TransferJob.Destination.BlobRequestOptions),
                         Utils.GenerateOperationContext(this.TransferContext),
                         this.CancellationToken);
            }
            else
            {
                if (BlobType.BlockBlob == this.destBlob.BlobType)
                {
                    return (this.destBlob as CloudBlockBlob).StartCopyAsync(
                             this.SourceFile.GenerateCopySourceFile(),
                             null,
                             destAccessCondition,
                             Utils.GenerateBlobRequestOptions(this.TransferJob.Destination.BlobRequestOptions),
                             Utils.GenerateOperationContext(this.TransferContext),
                             this.CancellationToken);
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
                    throw new InvalidOperationException(Resources.DestinationBlobTypeNotMatch);
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

        protected override async Task<CopyState> FetchCopyStateAsync()
        {
            await this.destBlob.FetchAttributesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.TransferJob.Destination.AccessCondition),
                Utils.GenerateBlobRequestOptions(this.TransferJob.Destination.BlobRequestOptions),
                Utils.GenerateOperationContext(this.TransferContext),
                this.CancellationToken);

            return this.destBlob.CopyState;
        }
    }
}
