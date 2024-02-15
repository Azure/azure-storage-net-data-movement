using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal sealed class UploadDirectorySingleTransferItemTransfer : SingleTransferItemTransferBase
    {
        public UploadDirectorySingleTransferItemTransfer(CommandLineOptions options)
            : base(options)
        {
        }

        protected override Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            var transferOptions = new UploadDirectoryOptions { Recursive = true };
            DirectoryTransferContext transferContext = new(Stream.Null);
            AttachEventsAndProgress(transferContext);
            AddAdditionalHashIfRequested(transferContext);

            var destination = CloudBlobContainer.GetDirectoryReference($"{RelativePath}/{JobId}");

            return TransferManager.UploadDirectoryAsync(Options.Source, destination, transferOptions, transferContext,
                token);
        }

        private void AddAdditionalHashIfRequested(TransferContext transferContext)
        {
            if (!Options.AddMd5ToMetadata) return;

            transferContext.SetAttributesCallbackAsync += UploadHelper.AddMd5ToMetadataDelegate;
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();
            ValidateUrl(Options.Destination, nameof(Options.Destination), "Destination should be http(s) url.");

            if (!Directory.Exists(Options.Source))
            {
                var ex = new TransferException( 
                    TransferErrorCode.DirectoryNotFound, 
                    "Directory does not exist.");

                ex.Data.Add("path", Options.Source);
                throw ex;
            }

        }
    }
}