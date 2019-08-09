//------------------------------------------------------------------------------
// <copyright file="CopyWrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement;

    internal class CopyWrapper : DMLibWrapper
    {
        public CopyWrapper()
        {
        }

        protected override async Task<TransferStatus> DoTransferImp(TransferItem item)
        {
            if (item.IsDirectoryTransfer)
            {
                return await this.CopyDirectory(item.SourceObject, item.DestObject, item);
            }
            else
            {
                await this.Copy(item.SourceObject, item.DestObject, item);
                return null;
            }
        }

        private Task Copy(dynamic sourceObject, dynamic destObject, TransferItem item)
        {
            CopyOptions copyOptions = item.Options as CopyOptions;
            SingleTransferContext transferContext = item.TransferContext as SingleTransferContext;
            CancellationToken cancellationToken = item.CancellationToken;

            if (cancellationToken != null && cancellationToken != CancellationToken.None)
            {
                if (sourceObject is Uri)
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod == CopyMethod.ServiceSideAsyncCopy, copyOptions, transferContext, cancellationToken);
                }
                else
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod, copyOptions, transferContext, cancellationToken);
                }
            }
            else if (transferContext != null || copyOptions != null)
            {
                if (sourceObject is Uri)
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod == CopyMethod.ServiceSideAsyncCopy, copyOptions, transferContext);
                }
                else
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod, copyOptions, transferContext);
                }
            }
            else
            {
                if (sourceObject is Uri)
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod == CopyMethod.ServiceSideAsyncCopy);
                }
                else
                {
                    return TransferManager.CopyAsync(sourceObject, destObject, item.CopyMethod);
                }
            }
        }

        private Task<TransferStatus> CopyDirectory(dynamic sourceObject, dynamic destObject, TransferItem item)
        {
            CopyDirectoryOptions copyDirectoryOptions = item.Options as CopyDirectoryOptions;
            DirectoryTransferContext transferContext = item.TransferContext as DirectoryTransferContext;
            CancellationToken cancellationToken = item.CancellationToken;

            if (cancellationToken == null || cancellationToken == CancellationToken.None)
            {
                return TransferManager.CopyDirectoryAsync(sourceObject, destObject, item.CopyMethod, copyDirectoryOptions, transferContext);
            }
            else
            {
                return TransferManager.CopyDirectoryAsync(sourceObject, destObject, item.CopyMethod, copyDirectoryOptions, transferContext, cancellationToken);
            }
        }
    }
}
