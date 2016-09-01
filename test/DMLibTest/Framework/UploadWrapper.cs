//------------------------------------------------------------------------------
// <copyright file="UploadWrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

    internal class UploadWrapper : DMLibWrapper
    {
        public UploadWrapper()
        {
        }

        protected override async Task<TransferStatus> DoTransferImp(TransferItem item)
        {
            if (item.IsDirectoryTransfer)
            {
                return await this.UploadDirectory(item.DestObject, item);
            }
            else
            {
                await this.Upload(item.DestObject, item);
                return null;
            }
        }

        private Task Upload(dynamic destObject, TransferItem item)
        {
            UploadOptions uploadOptions = item.Options as UploadOptions;
            TransferContext transferContext = item.TransferContext;
            CancellationToken cancellationToken = item.CancellationToken;
            string sourcePath = item.SourceObject as string;
            Stream sourceStream = item.SourceObject as Stream;

            if (cancellationToken != null && cancellationToken != CancellationToken.None)
            {
                if (sourcePath != null)
                {
                    return TransferManager.UploadAsync(sourcePath, destObject, uploadOptions, transferContext, cancellationToken);
                }
                else
                {
                    return TransferManager.UploadAsync(sourceStream, destObject, uploadOptions, transferContext, cancellationToken);
                }
            }
            else if (transferContext != null || uploadOptions != null)
            {
                if (sourcePath != null)
                {
                    return TransferManager.UploadAsync(sourcePath, destObject, uploadOptions, transferContext);
                }
                else
                {
                    return TransferManager.UploadAsync(sourceStream, destObject, uploadOptions, transferContext);
                }
            }
            else
            {
                if (sourcePath != null)
                {
                    return TransferManager.UploadAsync(sourcePath, destObject);
                }
                else
                {
                    return TransferManager.UploadAsync(sourceStream, destObject);
                }
            }
        }

        private Task<TransferStatus> UploadDirectory(dynamic destObject, TransferItem item)
        {
            UploadDirectoryOptions uploadDirectoryOptions = item.Options as UploadDirectoryOptions;
            TransferContext transferContrext = item.TransferContext;
            CancellationToken cancellationToken = item.CancellationToken;
            string sourcePath = item.SourceObject as string;

            if (cancellationToken != null && cancellationToken != CancellationToken.None)
            {
                return TransferManager.UploadDirectoryAsync(sourcePath, destObject, uploadDirectoryOptions, transferContrext, cancellationToken);
            }
            else if (transferContrext != null || uploadDirectoryOptions != null)
            {
                return TransferManager.UploadDirectoryAsync(sourcePath, destObject, uploadDirectoryOptions, transferContrext);
            }
            else
            {
                return TransferManager.UploadDirectoryAsync(sourcePath, destObject);
            }
        }
    }
}
