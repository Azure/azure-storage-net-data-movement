//------------------------------------------------------------------------------
// <copyright file="BlobDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;

    internal abstract class BlobDestHandler : IDestHandler
    {
        TransferJob transferJob;
        AzureBlobLocation destLocation;
        CloudBlob destBlob;
        TransferContext transferContext;

        protected AzureBlobLocation DestLocation
        {
            get
            {
                return this.destLocation;
            }
        }

        protected TransferContext TransferContext
        {
            get
            {
                return this.transferContext;
            }
        }
        public Uri Uri
        {
            get { return this.destBlob.Uri; }
        }

        public BlobDestHandler(AzureBlobLocation destLocation, TransferJob transferJob)
        {
            Debug.Assert(null != destLocation && null != transferJob,
                "destLocation or transferJob should not be null");
            this.destLocation = destLocation;
            this.transferJob = transferJob;
            this.destBlob = this.destLocation.Blob;
            this.transferContext = this.transferJob.Transfer.Context;
        }

        public async Task<bool> CheckAndCreateDestinationAsync(
            bool isForceOverwrite, 
            long totalLength,
            Func<bool, Task> checkOverwrite,
            CancellationToken cancellationToken)
        {
            bool needCreateDestination = true;
            bool gotDestAttributes = false;

            // Check access condition here no matter whether force overwrite is true.
            if (!this.destLocation.CheckedAccessCondition && null != this.destLocation.AccessCondition)
            {
                try
                {
                    await this.destBlob.FetchAttributesAsync(
                        Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, false),
                        Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.transferContext),
                        cancellationToken).ConfigureAwait(false);

                    // Only try to send the blob creating request, when blob length is not as expected. Otherwise, only need to clear all pages.
                    needCreateDestination = true;
                    this.destLocation.CheckedAccessCondition = true;
                    gotDestAttributes = true;

                    if (!isForceOverwrite)
                    {
                        await checkOverwrite(true).ConfigureAwait(false);
                    }
                }
                catch (StorageException se)
                {
                    if ((null == se.RequestInformation) || ((int)HttpStatusCode.NotFound != se.RequestInformation.HttpStatusCode))
                    {
                        throw;
                    }
                }
            }

            if (!isForceOverwrite)
            {
                if (this.transferJob.Overwrite.HasValue)
                {
                    await checkOverwrite(true).ConfigureAwait(false);
                }
                else
                {
                    AccessCondition accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");

                    try
                    {
                        await this.CreateDestinationAsync(totalLength, accessCondition, CancellationToken.None).ConfigureAwait(false);

                        needCreateDestination = false;
                        this.destLocation.CheckedAccessCondition = true;
                        this.transferJob.Overwrite = true;
                        this.transferJob.Transfer.UpdateJournal();
                    }
                    catch (StorageException se)
                    {
                        if ((null != se.RequestInformation) &&
                            (((int)HttpStatusCode.Conflict == se.RequestInformation.HttpStatusCode)
                                    && string.Equals(se.RequestInformation.ErrorCode, "BlobAlreadyExists")))
                        {
                            await checkOverwrite(true).ConfigureAwait(false);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
            else
            {
                gotDestAttributes = true;
            }

            Utils.CheckCancellation(cancellationToken);

            if (needCreateDestination)
            {
                this.transferJob.Overwrite = true;
                this.transferJob.Transfer.UpdateJournal();

                await this.CreateDestinationAsync(
                    totalLength,
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, true),
                    CancellationToken.None).ConfigureAwait(false);
            }

            return gotDestAttributes;
        }

        public virtual async Task CommitAsync(
            bool gotDestAttributes,
            Attributes sourceAttributes,
            Func<object, object, Task> setCustomAttributes,
            CancellationToken cancellationToken)
        {
            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.transferContext);

            if (!gotDestAttributes)
            {
                await this.destBlob.FetchAttributesAsync(
                     Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                     blobRequestOptions,
                     operationContext,
                     cancellationToken).ConfigureAwait(false);
            }

            var originalMetadata = new Dictionary<string, string>(this.destBlob.Metadata);

            Utils.SetAttributes(this.destBlob, sourceAttributes);

            await setCustomAttributes(this.transferJob.Source.Instance, this.destBlob).ConfigureAwait(false);

            await this.destBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                operationContext,
                cancellationToken).ConfigureAwait(false);

            if (!originalMetadata.DictionaryEquals(this.destBlob.Metadata))
            {
                await this.destBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        protected abstract Task CreateDestinationAsync(long totalLength, AccessCondition accessCondition, CancellationToken cancellationToken);
    }
}
