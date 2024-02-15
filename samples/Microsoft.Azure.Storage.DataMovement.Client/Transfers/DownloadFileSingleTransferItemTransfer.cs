using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal sealed class DownloadFileSingleTransferItemTransfer : SingleTransferItemTransferBase
    {
        public DownloadFileSingleTransferItemTransfer(CommandLineOptions options)
            : base(options)
        {
            var destinationDir = Path.Combine(Path.GetDirectoryName(options.Destination), JobId);
            if (!Directory.Exists(destinationDir)) Directory.CreateDirectory(destinationDir);
            var fileName = Path.GetFileName(Options.Destination);
            Destination = Path.Combine(destinationDir, fileName);
        }

        private string Destination { get; }

        protected override async Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            DownloadOptions transferOptions = new();
            SingleTransferContext transferContext = new(Stream.Null);
            AttachEventsAndProgress(transferContext);

            var source = CloudBlobContainer.GetBlockBlobReference(RelativePath);

            await TransferManager.DownloadAsync(source, Destination, transferOptions, transferContext, token)
                .ConfigureAwait(false);

            return new TransferStatus(source.Properties.Length, 1, 0, 0);
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();
            ValidateUrl(Options.Source, nameof(Options.Source), "Source should be http(s) url.");
        }
    }
}