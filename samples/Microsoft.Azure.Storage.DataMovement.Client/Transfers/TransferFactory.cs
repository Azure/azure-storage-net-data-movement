using System;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal class TransferFactory
    {
        private readonly CommandLineOptions options;

        public TransferFactory(CommandLineOptions options)
        {
            this.options = options;
        }

        public TransferBase Create()
        {
            switch (options.TransferType)
            {
                case TransferType.UploadDirectory: return new UploadDirectoryTransfer(options);
                case TransferType.UploadFile: return new UploadFileTransfer(options);
                case TransferType.DownloadDirectory: return new DownloadDirectoryTransfer(options);
                case TransferType.DownloadFile: return new DownloadFileTransfer(options);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}