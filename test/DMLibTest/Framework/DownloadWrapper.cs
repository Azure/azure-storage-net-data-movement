//------------------------------------------------------------------------------
// <copyright file="DownloadWrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement;

    internal class DownloadWrapper : DMLibWrapper
    {
        public DownloadWrapper()
        {
        }

        protected override async Task<TransferStatus> DoTransferImp(TransferItem item)
        {
            if (item.IsDirectoryTransfer)
            {
                return await this.DownloadDirectory(item.SourceObject, item);
            }
            else
            {
                await this.Download(item.SourceObject, item);
                return null;
            }
        }

        private Task Download(dynamic sourceObject, TransferItem item)
        {
            DownloadOptions downloadOptions = item.Options as DownloadOptions;
            SingleTransferContext transferContext = item.TransferContext as SingleTransferContext;
            CancellationToken cancellationToken = item.CancellationToken;
            string destPath = item.DestObject as string;
            Stream destStream = item.DestObject as Stream;

            if (cancellationToken != null && cancellationToken != CancellationToken.None)
            {
                if (destPath != null)
                {
                    return TransferManager.DownloadAsync(sourceObject, destPath, downloadOptions, transferContext, cancellationToken);
                }
                else
                {
                    return TransferManager.DownloadAsync(sourceObject, destStream, downloadOptions, transferContext, cancellationToken);
                }
            }
            else if (transferContext != null || downloadOptions != null)
            {
                if (destPath != null)
                {
                    return TransferManager.DownloadAsync(sourceObject, destPath, downloadOptions, transferContext);
                }
                else
                {
                    return TransferManager.DownloadAsync(sourceObject, destStream, downloadOptions, transferContext);
                }
            }
            else
            {
                if (destPath != null)
                {
                    return TransferManager.DownloadAsync(sourceObject, destPath);
                }
                else
                {
                    return TransferManager.DownloadAsync(sourceObject, destStream);
                }
            }
        }

        private Task<TransferStatus> DownloadDirectory(dynamic sourceObject, TransferItem item)
        {
            DownloadDirectoryOptions downloadDirectoryOptions = item.Options as DownloadDirectoryOptions;
            DirectoryTransferContext transferContext = item.TransferContext as DirectoryTransferContext;
            CancellationToken cancellationToken = item.CancellationToken;
            string destPath = item.DestObject as string;

            if (cancellationToken == null || cancellationToken == CancellationToken.None)
            {
                return TransferManager.DownloadDirectoryAsync(sourceObject, destPath, downloadDirectoryOptions, transferContext);
            }
            else
            {
                return TransferManager.DownloadDirectoryAsync(sourceObject, destPath, downloadDirectoryOptions, transferContext, cancellationToken);
            }
        }
    }
}
