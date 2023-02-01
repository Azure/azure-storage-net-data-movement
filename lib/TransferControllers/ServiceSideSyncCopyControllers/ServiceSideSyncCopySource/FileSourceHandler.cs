//------------------------------------------------------------------------------
// <copyright file="FileSourceHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopySource
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.File;
    
    class FileSourceHandler : IRangeBasedSourceHandler
    {
        private AzureFileLocation sourceLocation;
        private TransferContext transferContext;
        private CloudFile sourceFile;
        private TransferJob transferJob;
        private Attributes sourceAttributes;
        private long totalLength;

        public FileSourceHandler(AzureFileLocation sourceLocation, TransferJob transferJob)
        {
            this.sourceLocation = sourceLocation;
            this.transferJob = transferJob;
            this.sourceFile = this.sourceLocation.AzureFile;
            this.transferContext = this.transferJob.Transfer.Context;
        }

        public string ETag
        {
            get { return this.sourceLocation.ETag; }
        }

        public AccessCondition AccessCondition
        {
            get { return this.sourceLocation.AccessCondition; }
        }

        public Uri Uri
        {
            get { return this.sourceFile.Uri; }
        }

        public bool NeedToCheckAccessCondition
        {
            get { return false; }
        }

        public Task DownloadRangeToStreamAsync(Stream stream,
            long startOffset,
            long length,
            AccessCondition accessCondition,
            bool useTransactionalMD5,
            OperationContext operationContext,
            CancellationToken cancellationToken)
        {
            return this.sourceFile.DownloadRangeToStreamAsync(stream, startOffset, length, accessCondition, new FileRequestOptions
                {
                    UseTransactionalMD5 = useTransactionalMD5
                },
                operationContext,
                cancellationToken);
        }

        public async Task FetchAttributesAsync(CancellationToken cancellationToken)
        {
            try
            {
                await this.sourceFile.FetchAttributesAsync(
                    null,
                    Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                    Utils.GenerateOperationContext(this.transferContext),
                    cancellationToken);
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
            catch (StorageException e)
            {
#endif
                HandleFetchSourceAttributesException(e);
                throw;
            }

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if (0 != this.transferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceFile.Properties.ETag;
            }
            else if ((this.transferJob.CheckPoint.EntryTransferOffset > this.sourceFile.Properties.Length)
                 || (this.transferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            SingleObjectTransfer transferInstance = this.transferJob.Transfer;

            this.sourceAttributes = Utils.GenerateAttributes(this.sourceFile, transferInstance.PreserveSMBAttributes);

            if (transferInstance.PreserveSMBPermissions != PreserveSMBPermissions.None)
            {
                if (!string.IsNullOrEmpty(this.sourceFile.FilePermission))
                {
                    this.sourceAttributes.PortableSDDL = this.sourceFile.FilePermission;
                }
                else if (!string.IsNullOrEmpty(this.sourceFile.Properties.FilePermissionKey))
                {
                    var sddlCache = transferInstance.SDDLCache;

                    if (null != sddlCache)
                    {
                        string portableSDDL = null;
                        sddlCache.TryGetValue(this.sourceFile.Properties.FilePermissionKey, out portableSDDL);

                        if (null == portableSDDL)
                        {
                            portableSDDL = await this.sourceFile.Share.GetFilePermissionAsync(this.sourceFile.Properties.FilePermissionKey,
                                Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                                Utils.GenerateOperationContext(this.transferContext),
                                cancellationToken).ConfigureAwait(false);

                            sddlCache.TryAddValue(this.sourceFile.Properties.FilePermissionKey, portableSDDL);
                        }

                        this.sourceAttributes.PortableSDDL = portableSDDL;
                    }
                    else
                    {
                        this.sourceAttributes.PortableSDDL = await this.sourceFile.Share.GetFilePermissionAsync(this.sourceFile.Properties.FilePermissionKey,
                            Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                            Utils.GenerateOperationContext(this.transferContext),
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    this.sourceAttributes.PortableSDDL = null;
                }
            }

            this.totalLength = this.sourceFile.Properties.Length;
        }

        private static void HandleFetchSourceAttributesException(StorageException e)
        {
            // Getting a storage exception is expected if the source doesn't
            // exist. For those cases that indicate the source doesn't exist
            // we will set a specific error state.
            if (e?.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException(Resources.SourceDoesNotExistException, e);
            }
        }


        public async Task<List<Utils.Range>> GetCopyRangesAsync(long startOffset, long length, CancellationToken cancellationToken)
        {
            var fileRanges = await this.sourceFile.ListRangesAsync(
                startOffset,
                length,
                null,
                Utils.GenerateFileRequestOptions(this.sourceLocation.FileRequestOptions),
                Utils.GenerateOperationContext(this.transferContext),
                cancellationToken);

            List<Utils.Range> ranges = new List<Utils.Range>();

            foreach (var fileRange in fileRanges)
            {
                ranges.Add(new Utils.Range()
                {
                    StartOffset = fileRange.StartOffset,
                    EndOffset = fileRange.EndOffset,
                    HasData = true
                });
            }

            return ranges;
        }

        public Uri GetCopySourceUri()
        {
            return this.sourceFile.GenerateCopySourceUri();
        }

        public Attributes SourceAttributes
        {
            get
            {
                return this.sourceAttributes;
            }
        }

        public long TotalLength
        {
            get
            {
                return this.totalLength;
            }
        }
    }
}
