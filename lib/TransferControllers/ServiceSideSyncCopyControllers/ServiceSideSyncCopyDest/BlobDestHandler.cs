//------------------------------------------------------------------------------
// <copyright file="BlobDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System;
    using System.Collections.Generic;
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

        public BlobDestHandler(AzureBlobLocation destLocation, TransferJob transferJob)
        {
            this.destLocation = destLocation;
            this.transferJob = transferJob;
            this.destBlob = this.destLocation.Blob;
            this.transferContext = this.transferJob.Transfer.Context;
        }

        public async Task<bool> CheckAndCreateDestinationAsync(
            bool isForceOverwrite, 
            long totalLength,
            Func<bool, Task> checkOverWrite,
            CancellationToken cancellationToken)
        {
            bool needCreateDestination = true;
            bool gotDestAttributes = false;
            if (!isForceOverwrite)
            {
                if (this.transferJob.Overwrite.HasValue)
                {
                    await checkOverWrite(true);
                }
                else if (!this.destLocation.CheckedAccessCondition && null != this.destLocation.AccessCondition)
                {
                    try
                    {
                        await this.destBlob.FetchAttributesAsync(
                            Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, false),
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.transferContext),
                            cancellationToken);

                        // Only try to send the blob creating request, when blob length is not as expected. Otherwise, only need to clear all pages.
                        needCreateDestination = (this.destBlob.Properties.Length != totalLength);
                        this.destLocation.CheckedAccessCondition = true;
                        gotDestAttributes = true;

                        await checkOverWrite(true);
                    }
                    catch (StorageException se)
                    {
                        if ((null == se.RequestInformation) || ((int)HttpStatusCode.NotFound != se.RequestInformation.HttpStatusCode))
                        {
                            throw;
                        }
                    }
                }
                else
                {
                    AccessCondition accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");

                    try
                    {
                        await this.CreateDestinationAsync(totalLength, accessCondition, CancellationToken.None);

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
                            await checkOverWrite(true);
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
                await this.CreateDestinationAsync(
                    totalLength,
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    cancellationToken);

                this.transferJob.Overwrite = true;
                this.destLocation.CheckedAccessCondition = true;
                this.transferJob.Transfer.UpdateJournal();
            }

            return gotDestAttributes;
        }

        public virtual async Task CommitAsync(
            bool gotDestAttributes,
            Attributes sourceAttributes,
            Func<object, Task> setCustomAttributes,
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
                     cancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.destBlob.Metadata);

            Utils.SetAttributes(this.destBlob, sourceAttributes);

            await setCustomAttributes(this.destBlob);

            await this.destBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                operationContext,
                cancellationToken);

            if (!originalMetadata.DictionaryEquals(this.destBlob.Metadata))
            {
                await this.destBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    cancellationToken);
            }
        }

        protected abstract Task CreateDestinationAsync(long totalLength, AccessCondition accessCondition, CancellationToken cancellationToken);
    }
}
