using System;
using System.Linq;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
	internal abstract class SingleTransferItemTransferBase : TransferBase<CommandLineOptions>
    {
        protected SingleTransferItemTransferBase(CommandLineOptions options) :base(options)
        {
            var uri = new Uri(GetRemotePath());
            StorageUri = new StorageUri(new Uri(uri.GetLeftPart(UriPartial.Authority)));
            Container = uri.Segments[1].TrimEnd('/');
            RelativePath = string.Join("/", uri.Segments.Skip(2).Select(x => x.TrimEnd('/')));
        }

        protected string RelativePath { get; }

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

        protected override string GetSasToken() => Options.GetSasToken();

        protected override void ValidateImpl()
        {
            if (string.IsNullOrWhiteSpace(Options.Source)) throw new ArgumentNullException(nameof(Options.Source));
            if (string.IsNullOrWhiteSpace(Options.Destination))
                throw new ArgumentNullException(nameof(Options.Destination));
        }
    }
}