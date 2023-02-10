using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal sealed class DownloadDirectoryTransfer : TransferBase
    {
        public DownloadDirectoryTransfer(CommandLineOptions options)
            : base(options)
        {
            Destination = Path.Combine(Options.Destination, JobId);
            if (!Directory.Exists(Destination)) Directory.CreateDirectory(Destination);
        }

        private string Destination { get; }

        protected override Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            var transferOptions = new DownloadDirectoryOptions { Recursive = true };
            DirectoryTransferContext transferContext = new(Stream.Null);
            AttachEventsAndProgress(transferContext);

            var source = CloudBlobContainer.GetDirectoryReference(RelativePath);

            return TransferManager.DownloadDirectoryAsync(source, Destination, transferOptions, transferContext, token);
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();
            ValidateUrl(Options.Source, nameof(Options.Source), "Source should be http(s) url.");
        }
    }
}