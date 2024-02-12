using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            var uri = new Uri(Options.Destination);
            StorageUri = new StorageUri(new Uri(uri.GetLeftPart(UriPartial.Authority)));
            Container = uri.Segments[1].TrimEnd('/');
            RelativePath = string.Join("/", uri.Segments.Skip(2).Select(x => x.TrimEnd('/')));
        }
        
        protected override string GetSasToken()
        {
            return Options.SasToken;
        }

        protected override Task<TransferStatus> ExecuteImplAsync(CancellationToken token)
        {
            var journalPath = Path.Combine(Path.GetTempPath(), JobId);
            var journal = new FileStream(journalPath, FileMode.OpenOrCreate);
            var transferOptions = new UploadDirectoryOptions { Recursive = true };
            DirectoryTransferContext transferContext = new(journal);
            AttachEventsAndProgress(transferContext);
            var transferItems = ReadLoadFile();
            var destination = CloudBlobContainer.GetDirectoryReference($"{RelativePath}/{JobId}");

            return TransferManager.UploadAsync(transferItems, destination, transferOptions, transferContext,
                token);
        }

        private IEnumerable<TransferItem> ReadLoadFile()
        {
            using var sr = new StreamReader(Options.Source);
            while (sr.ReadLine() is { } line)
            {
                var parts = line.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                yield return new TransferItem(parts[0], parts[1]);
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