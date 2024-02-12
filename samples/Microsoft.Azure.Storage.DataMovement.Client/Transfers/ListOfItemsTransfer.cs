using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;
using Microsoft.Azure.Storage.DataMovement.Dto;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal class ListOfItemsTransfer : TransferBase<ListOfItemsCommandLineOptions>
    {
        public ListOfItemsTransfer(ListOfItemsCommandLineOptions options) : base(options)
        {
            var uri = new Uri(GetRemotePath());
            StorageUri = new StorageUri(new Uri(uri.GetLeftPart(UriPartial.Authority)));
            Container = uri.Segments[1].TrimEnd('/');
        }

        private string GetRemotePath()
        {
            using var sw = new StreamReader(Options.Source);
            var line = sw.ReadLine();

            return line == null
                ? throw new ArgumentOutOfRangeException("Load file is empty.")
                : line.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)[1];
        }

        protected override string GetSasToken()
        {
            return Options.SasToken;
        }

        protected override Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            var transferOptions = new UploadDirectoryOptions { Recursive = true };
            DirectoryTransferContext transferContext = new(Stream.Null);
            AttachEventsAndProgress(transferContext);

            return TransferManager.UploadAsync(ReadLoadFile(), transferOptions, transferContext,
                token);
        }

        private IEnumerable<TransferItem> ReadLoadFile()
        {
            using var sr = new StreamReader(Options.Source);
            var credentials = CloudBlobContainer.ServiceClient.Credentials;

            while (sr.ReadLine() is { } line)
            {
                var parts = line.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                var blobFile = new CloudBlockBlob(new Uri(parts[1]), credentials);

                yield return new TransferItem(parts[0], blobFile);
            }
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();
            if (System.IO.File.Exists(Options.Source)) return;

            throw new FileNotFoundException("Load file does not exists");
        }
    }
}