//------------------------------------------------------------------------------
// <copyright file="CloudFileWriter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.File;

    internal sealed class CloudFileWriter : RangeBasedWriter
    {
        private AzureFileLocation destLocation;
        private CloudFile cloudFile;

        /// <summary>
        /// To indicate whether the destination already exist before this writing.
        /// If no, when try to set destination's attribute, should get its attributes first.
        /// </summary>
        private bool destExist = false;

        internal CloudFileWriter(
            TransferScheduler scheduler,
            SyncTransferController controller,
            CancellationToken cancellationToken)
            : base(scheduler, controller, cancellationToken)
        {
            this.destLocation = this.TransferJob.Destination as AzureFileLocation;
            this.cloudFile = this.destLocation.AzureFile;
        }

        protected override Uri DestUri
        {
            get 
            {
                return this.cloudFile.SnapshotQualifiedUri;
            }
        }

        protected override void CheckInputStreamLength(long inputStreamLength)
        {
            if (inputStreamLength < 0)
            {
                throw new TransferException(
                    TransferErrorCode.UploadFileSourceFileSizeInvalid,
                    string.Format(CultureInfo.CurrentCulture, Resources.SourceMustBeFixedSize, Resources.AzureFile));
            }

            return;
        }

        protected override async Task DoFetchAttributesAsync()
        {
            await this.cloudFile.FetchAttributesAsync(
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken).ConfigureAwait(false);
            this.destExist = true;
        }

        protected override void HandleFetchAttributesResult(Exception e)
        {
            // Do nothing here.
        }

        protected override async Task DoCreateAsync(long size)
        {
            if (this.destExist)
            {
                this.CleanupPropertyForCanonicalization();
            }

            try
            {
                var operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);
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

                await this.cloudFile.CreateAsync(
                    size,
                    null,
                    Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions, true),
                    operationContext,
                    this.CancellationToken).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                if ((null != ex.RequestInformation)
                    && (ex.RequestInformation.HttpStatusCode == (int)(HttpStatusCode.BadRequest))
                    && string.Equals(ex.RequestInformation.ErrorCode, Constants.FileSizeOutOfRangeErrorCode))
                {
                    throw new TransferException(
                        TransferErrorCode.UploadSourceFileSizeTooLarge,
                        Resources.CloudFileSizeTooLargeException,
                        ex);
                }
                else
                {
                    throw;
                }
            }
        }

        protected override async Task WriteRangeAsync(TransferData transferData)
        {
            await this.cloudFile.WriteRangeAsync(
                transferData.Stream,
                transferData.StartOffset,
                null,
                null,
                Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.Controller.TransferContext),
                this.CancellationToken).ConfigureAwait(false);
        }

        protected override async Task DoCommitAsync()
        {
            FileRequestOptions fileRequestOptions = Utils.GenerateFileRequestOptions(this.destLocation.FileRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.Controller.TransferContext);

            if (!this.Controller.IsForceOverwrite && !this.destExist)
            {
                await this.cloudFile.FetchAttributesAsync(
                    null,
                    fileRequestOptions,
                    operationContext,
                    this.CancellationToken).ConfigureAwait(false);
            }

            var originalMetadata = new Dictionary<string, string>(this.cloudFile.Metadata);

            if ((PreserveSMBPermissions.None != this.TransferJob.Transfer.PreserveSMBPermissions)
                && !string.IsNullOrEmpty(this.SharedTransferData.Attributes.PortableSDDL))
            {
                if (this.SharedTransferData.Attributes.PortableSDDL.Length >= Constants.MaxSDDLLengthInProperties)
                {
                    string permissionKey = null;
                    var sddlCache = this.TransferJob.Transfer.SDDLCache;
                    if (null != sddlCache)
                    {
                        sddlCache.TryGetValue(this.SharedTransferData.Attributes.PortableSDDL, out permissionKey);

                        if (null == permissionKey)
                        {
                            permissionKey = await this.cloudFile.Share.CreateFilePermissionAsync(this.SharedTransferData.Attributes.PortableSDDL,
                                fileRequestOptions,
                                operationContext,
                                this.CancellationToken).ConfigureAwait(false);

                            sddlCache.TryAddValue(this.SharedTransferData.Attributes.PortableSDDL, permissionKey);
                        }
                    }
                    else
                    {
                        permissionKey = await this.cloudFile.Share.CreateFilePermissionAsync(this.SharedTransferData.Attributes.PortableSDDL,
                            fileRequestOptions,
                            operationContext,
                            this.CancellationToken).ConfigureAwait(false);
                    }

                    this.cloudFile.Properties.FilePermissionKey = permissionKey;
            }
            else
            {
                this.cloudFile.FilePermission = this.SharedTransferData.Attributes.PortableSDDL;
            }
        }

            Utils.SetAttributes(this.cloudFile, this.SharedTransferData.Attributes, this.TransferJob.Transfer.PreserveSMBAttributes);
            await this.Controller.SetCustomAttributesAsync(this.TransferJob.Source.Instance, this.cloudFile).ConfigureAwait(false);

            await this.cloudFile.SetPropertiesAsync(
                null,
                fileRequestOptions,
                operationContext,
                this.CancellationToken).ConfigureAwait(false);

            if (!originalMetadata.DictionaryEquals(this.cloudFile.Metadata))
            {
                await this.cloudFile.SetMetadataAsync(
                    null,
                    fileRequestOptions,
                    operationContext,
                    this.CancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Cleanup properties that might cause request canonicalization check failure.
        /// </summary>
        private void CleanupPropertyForCanonicalization()
        {
            this.cloudFile.Properties.ContentLanguage = null;
            this.cloudFile.Properties.ContentEncoding = null;
        }
    }
}
