//------------------------------------------------------------------------------
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
        public Uri Uri
        {
            get { return this.destFile.Uri; }
        }

        public async Task<bool> CheckAndCreateDestinationAsync(
            bool isForceOverwrite,
            long totalLength,
            Func<bool, Task> checkOverwrite,
            CancellationToken cancellationToken)
        {
            bool needCreateDestination = true;
            bool gotDestAttributes = false;
            if (!isForceOverwrite)
            {
                if (this.transferJob.Overwrite.HasValue)
                {
                    await checkOverwrite(true);
                }
                else 
                {
                    try
                    {
                        await this.destFile.FetchAttributesAsync(
                            null,
                            Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                            Utils.GenerateOperationContext(this.transferContext),
                            cancellationToken);

                        // Only try to send the blob creating request, when blob length is not as expected. Otherwise, only need to clear all pages.
                        needCreateDestination = (this.destFile.Properties.Length != totalLength);
                        this.destLocation.CheckedAccessCondition = true;
                        gotDestAttributes = true;

                        await checkOverwrite(true);
                    }
                    catch (StorageException se)
                    {
                        if ((null == se.RequestInformation) || ((int)HttpStatusCode.NotFound != se.RequestInformation.HttpStatusCode))
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
                    null,
                    CancellationToken.None);
            }

            return gotDestAttributes;
        }

        public virtual async Task CommitAsync(
            bool gotDestAttributes,
            Attributes sourceAttributes,
            Func<object, object, Task> setCustomAttributes,
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

            SingleObjectTransfer transferInstance = this.transferJob.Transfer;

            Utils.SetAttributes(this.destFile, sourceAttributes, transferInstance.PreserveSMBAttributes);

            if ((PreserveSMBPermissions.None != transferInstance.PreserveSMBPermissions)
                && !string.IsNullOrEmpty(sourceAttributes.PortableSDDL))
            {
                if (sourceAttributes.PortableSDDL.Length >= Constants.MaxSDDLLengthInProperties)
                {
                    string permissionKey = null;
                    var sddlCache = transferInstance.SDDLCache;
                    if (null != sddlCache)
                    {
                        sddlCache.TryGetValue(sourceAttributes.PortableSDDL, out permissionKey);

                        if (null == permissionKey)
                        {
                            permissionKey = await this.destFile.Share.CreateFilePermissionAsync(sourceAttributes.PortableSDDL,
                                fileRequestOptions,
                                operationContext,
                                cancellationToken).ConfigureAwait(false);

                            sddlCache.TryAddValue(sourceAttributes.PortableSDDL, permissionKey);
                        }
                    }
                    else
                    {
                        permissionKey = await this.destFile.Share.CreateFilePermissionAsync(sourceAttributes.PortableSDDL,
                            fileRequestOptions,
                            operationContext,
                            cancellationToken).ConfigureAwait(false);
                    }

                    this.destFile.Properties.FilePermissionKey = permissionKey;
                }
                else
                {
                    this.destFile.FilePermission = sourceAttributes.PortableSDDL;
                }
            }

            await setCustomAttributes(this.transferJob.Source.Instance, this.destFile);

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
            var operationContext = Utils.GenerateOperationContext(this.transferContext);
            operationContext.SendingRequest += (sender, eventArgs) =>
            {
                eventArgs.Request.Headers.Remove(Constants.MSVersionHeaderName);
                eventArgs.Request.Headers.Add(Constants.MSVersionHeaderName, Constants.LargeSMBFileVersion);
            };

            operationContext.Retrying += (sender, eventArgs) =>
            {
                eventArgs.Request.Headers.Remove(Constants.MSVersionHeaderName);
                eventArgs.Request.Headers.Add(Constants.MSVersionHeaderName, Constants.LargeSMBFileVersion);
            };

            return this.destFile.CreateAsync(
                    totalLength,
                    accessCondition,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                    operationContext,
                    cancellationToken);
        }
    }
}
