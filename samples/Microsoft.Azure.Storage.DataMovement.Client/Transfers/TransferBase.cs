using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement.Client.Logger;
using Microsoft.Azure.Storage.DataMovement.Client.Progress;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal abstract class TransferBase
    {
        protected TransferBase(CommandLineOptions options)
        {
            Options = options;

            Validate();

            var uri = new Uri(GetRemotePath());
            JobId = Guid.NewGuid().ToString();
            StorageUri = new Uri(uri.GetLeftPart(UriPartial.Authority));
            Container = uri.Segments[1].TrimEnd('/');
            RelativePath = string.Join("/", uri.Segments.Skip(2).Select(x => x.TrimEnd('/')));
        }


        protected CommandLineOptions Options { get; }
        protected string RelativePath { get; }
        protected CloudBlobContainer CloudBlobContainer { get; private set; }

        public string JobId { get; }
        private Uri StorageUri { get; }
        private string Container { get; }

        private string GetRemotePath()
        {
            return Options.TransferType switch
            {
                TransferType.UploadDirectory => Options.Destination,
                TransferType.UploadFile => Options.Destination,
                TransferType.DownloadDirectory => Options.Source,
                TransferType.DownloadFile => Options.Source,
                _ => throw new ArgumentOutOfRangeException(nameof(Options.TransferType))
            };
        }

        public Task<TransferStatus> ExecuteAsync(CancellationToken token)
        {
            EnsureContainerClient();

            return ExecuteImplAsync(token);
        }

        protected virtual void ValidateImpl()
        {
        }

        private void Validate()
        {
            if (string.IsNullOrWhiteSpace(Options.Source)) throw new ArgumentNullException(nameof(Options.Source));
            if (string.IsNullOrWhiteSpace(Options.Destination))
                throw new ArgumentNullException(nameof(Options.Destination));

            ValidateImpl();
        }

        protected abstract Task<TransferStatus> ExecuteImplAsync(CancellationToken token);

        protected void AttachEventsAndProgress(TransferContext transferContext)
        {
            const string failedTypeMsg = "File failed";
            const string skippedTypeMsg = "File skipped";

            transferContext.ClientRequestId = JobId;
            transferContext.FileFailed += (_, e) => e.LogFailedOrSkipped(JobId, failedTypeMsg);
            transferContext.FileSkipped += (_, e) => e.LogFailedOrSkipped(JobId, skippedTypeMsg);
            transferContext.ProgressHandler = new TimedProgressStatus(JobId);
        }

        private void EnsureContainerClient()
        {
            var sasToken = Options.GetSasToken();
            var cloudBlobClient = new CloudBlobClient(new StorageUri(StorageUri), new StorageCredentials(sasToken));
            CloudBlobContainer = cloudBlobClient.GetContainerReference(Container);
        }

        protected static void ValidateUrl(string possibleUrl, string parameterName, string message)
        {
            var prefixes = new[] { "http://", "https://" };
            if (!prefixes.Any(x => possibleUrl.StartsWith(x, StringComparison.OrdinalIgnoreCase)))
                throw new ArgumentOutOfRangeException(parameterName, message);
        }
    }
}