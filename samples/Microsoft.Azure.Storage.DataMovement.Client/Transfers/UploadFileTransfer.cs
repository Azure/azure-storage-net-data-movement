using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal sealed class UploadFileTransfer : TransferBase
    {
        public UploadFileTransfer(CommandLineOptions options)
            : base(options)
        {
        }

        protected override async Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            UploadOptions transferOptions = new();
            SingleTransferContext transferContext = new(Stream.Null);
            AttachEventsAndProgress(transferContext);
            AddAdditionalHashIfRequested(transferContext);

            var remoteDir = Path.GetDirectoryName(RelativePath);
            var remoteFileName = Path.GetFileName(RelativePath);
            var destination = CloudBlobContainer.GetBlockBlobReference($"{remoteDir}/{JobId}/{remoteFileName}");

            await TransferManager.UploadAsync(Options.Source, destination, transferOptions, transferContext, token)
                .ConfigureAwait(false);

            var length = new FileInfo(Options.Source).Length;

            return new TransferStatus(length, 1, 0, 0);
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();
            ValidateUrl(Options.Destination, nameof(Options.Destination), "Destination should be http(s) url.");

            if (!System.IO.File.Exists(Options.Source))
                throw new FileNotFoundException($"File ({Options.Source}) does not exist.");
        }

        private void AddAdditionalHashIfRequested(TransferContext transferContext)
        {
            if (!Options.AddMd5ToMetadata) return;

            transferContext.SetAttributesCallbackAsync += UploadHelper.AddMd5ToMetadataDelegate;
        }
    }
}