using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;
using Microsoft.Azure.Storage.DataMovement.Client.Logger;
using Microsoft.Azure.Storage.DataMovement.Client.Progress;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal abstract class TransferBase<TOptions> : ITransfer
    where TOptions:IBaseOptions
    {
        protected TransferBase(TOptions options)
        {
            Options = options;
            ValidateImpl();

            var jobId = Options.JobId ?? Guid.NewGuid();
            JobId = jobId.ToString();
        }


        protected TOptions Options { get; }
        protected CloudBlobContainer CloudBlobContainer { get; private set; }
        protected string Container { get; set; }
        protected StorageUri StorageUri { get; set; }
        protected string RelativePath { get; set; }

        public string JobId { get; }

        public Task<TransferStatus> ExecuteAsync(CancellationToken token)
        {
            EnsureContainerClient();

            return ExecuteImplAsync(token);
        }

        protected virtual void ValidateImpl()
        {
        }

        protected abstract string GetSasToken();
        protected abstract Task<TransferStatus> ExecuteImplAsync(CancellationToken token);

        protected void AttachEventsAndProgress(TransferContext transferContext)
        {
            const string failedTypeMsg = "File failed";
            const string skippedTypeMsg = "File skipped";

            if (Options.AddConsoleLogger) transferContext.Logger = new ConsoleLogger();

            transferContext.ClientRequestId = JobId;
            transferContext.FileFailed += (_, e) => e.LogFailedOrSkipped(JobId, failedTypeMsg);
            transferContext.FileSkipped += (_, e) => e.LogFailedOrSkipped(JobId, skippedTypeMsg);
            transferContext.ProgressHandler = new TimedProgressStatus(JobId);
        }

        private void EnsureContainerClient()
        {
            var storageCredentials = new StorageCredentials(GetSasToken());
            var cloudBlobClient = new CloudBlobClient(StorageUri, storageCredentials);
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