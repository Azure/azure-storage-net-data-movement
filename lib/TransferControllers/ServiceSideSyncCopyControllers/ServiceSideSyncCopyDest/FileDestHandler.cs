﻿//------------------------------------------------------------------------------
// <copyright file="FileDestHandler.cs" company="Microsoft">
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
    using Microsoft.Azure.Storage.File;

    internal class FileDestHandler : IDestHandler
    {
        TransferJob transferJob;
        AzureFileLocation destLocation;
        CloudFile destFile;
        TransferContext transferContext;

        public FileDestHandler(AzureFileLocation destLocation, TransferJob transferJob)
        {
            this.destLocation = destLocation;
            this.transferJob = transferJob;
            this.transferContext = this.transferJob.Transfer.Context;
            this.destFile = this.destLocation.AzureFile;
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
                        await this.destFile.FetchAttributesAsync(
                            Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, false),
                            Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                            Utils.GenerateOperationContext(this.transferContext),
                            cancellationToken);

                        // Only try to send the blob creating request, when blob length is not as expected. Otherwise, only need to clear all pages.
                        needCreateDestination = (this.destFile.Properties.Length != totalLength);
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
                    try
                    {
                        await this.CreateDestinationAsync(totalLength, null, CancellationToken.None);

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
            FileRequestOptions fileRequestOptions = Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.transferContext);

            if (!gotDestAttributes)
            {
                await this.destFile.FetchAttributesAsync(
                     Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                     fileRequestOptions,
                     operationContext,
                     cancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.destFile.Metadata);

            Utils.SetAttributes(this.destFile, sourceAttributes, false);

            await setCustomAttributes(this.destFile);

            await this.destFile.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                fileRequestOptions,
                operationContext,
                cancellationToken);

            if (!originalMetadata.DictionaryEquals(this.destFile.Metadata))
            {
                await this.destFile.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    fileRequestOptions,
                    operationContext,
                    cancellationToken);
            }
        }

        private Task CreateDestinationAsync(long totalLength, AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return this.destFile.CreateAsync(
                    totalLength,
                    accessCondition,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    Utils.GenerateOperationContext(this.transferContext),
                    cancellationToken);
        }
    }
}
