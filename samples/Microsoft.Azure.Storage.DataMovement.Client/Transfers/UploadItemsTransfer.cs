using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.DataMovement.Dto;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal sealed class UploadItemsTransfer : TransferBase
    {
        private readonly IEnumerable<TransferItem> transferItems;

        public UploadItemsTransfer(CommandLineOptions options, IEnumerable<TransferItem> transferItems)
            : base(options)
        {
            this.transferItems = transferItems;
        }

        protected override Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            var journalPath = Path.Combine(Path.GetTempPath(), JobId);
            var journal = new FileStream(journalPath, FileMode.OpenOrCreate);
            DirectoryTransferContext transferContext = new(journal);
            AttachEventsAndProgress(transferContext);
            AddAdditionalHashIfRequested(transferContext);

            var destination = CloudBlobContainer.GetDirectoryReference($"{RelativePath}/{JobId}");

            var uploadTask = TransferManager.UploadAsync(transferItems, destination, null, transferContext,
                token);
            
            uploadTask.ContinueWith(t =>
            {
                journal.Flush();
                journal.Dispose();
            });

            return uploadTask;
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
        }
    }
}