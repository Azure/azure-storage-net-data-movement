using System;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal class TransferFactory
    {
        private readonly ITransferTypeOptions transferTypeOptions;

        public TransferFactory(ITransferTypeOptions transferTypeOptions)
        {
            this.transferTypeOptions = transferTypeOptions;
        }

        public ITransfer Create()
        {
            switch (transferTypeOptions.TransferType)
            {
                case TransferType.UploadDirectory: return new UploadDirectorySingleTransferItemTransfer((CommandLineOptions)transferTypeOptions);
                case TransferType.UploadFile: return new UploadFileSingleTransferItemTransfer((CommandLineOptions)transferTypeOptions);
                case TransferType.DownloadDirectory: return new DownloadDirectorySingleTransferItemTransfer((CommandLineOptions)transferTypeOptions);
                case TransferType.DownloadFile: return new DownloadFileSingleTransferItemTransfer((CommandLineOptions)transferTypeOptions);
                case TransferType.ListOfItems: return new ListOfItemsTransfer((ListOfItemsCommandLineOptions)transferTypeOptions);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}