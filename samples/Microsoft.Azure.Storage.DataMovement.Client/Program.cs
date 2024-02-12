using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement.Client.Logger;
using Microsoft.Azure.Storage.DataMovement.Client.Transfers;
using Microsoft.Azure.Storage.DataMovement.Dto;

namespace Microsoft.Azure.Storage.DataMovement.Client
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            var options = ParseArguments(args);
            if (options == null) return;

            Console.WriteLine($"Starting {GetTransferType(options.TransferType)} transfer:");
            Console.WriteLine($"  from: {options.Source}");
            Console.WriteLine($"  to: {options.Destination}");
            Console.WriteLine($"  transfer's number: {options.TransfersNumber}");
            Console.WriteLine($"{GetHashDescription(options)}");
            Console.WriteLine($"DMLib version: {typeof(TransferManager).Assembly.GetName().Version}{Environment.NewLine}");

            using var cts = new CancellationTokenSource();
            HookCancel(cts);
            var transferFactory = new TransferFactory(options);
            var elapsed = new Stopwatch();
            elapsed.Start();
            
            var tasks = Enumerable.Range(0, options.TransfersNumber).Select(async i =>
            {
                var transferItems = GetTransferItems();//.Take(1);
                var transfer = new UploadItemsTransfer(options, transferItems);
                var result = await transfer.ExecuteAsync(cts.Token).ConfigureAwait(false);
            
                result.PrintResult(transfer.JobId);
            });            
            
            // var tasks = Enumerable.Range(0, options.TransfersNumber).Select(async i =>
            // {
            //     var transfer = transferFactory.Create();
            //     var result = await transfer.ExecuteAsync(cts.Token).ConfigureAwait(false);
            //
            //     result.PrintResult(transfer.JobId);
            // });

            await Task.WhenAll(tasks).ConfigureAwait(false);

            elapsed.Stop();

            Console.WriteLine(
                $"{Environment.NewLine}All transfers ended up. Transfers took {elapsed.Elapsed.TotalSeconds:0} seconds");
        }

        private static IEnumerable<TransferItem> GetTransferItems()
        {
            var root = @"C:\temp\small";
            var entries = Directory.GetFileSystemEntries(root, "*", SearchOption.AllDirectories);
            foreach (var entry in entries)
            {
                if (System.IO.File.Exists(entry))
                {
                    var dst = entry.Substring(root.Length + 1);
                    yield return new TransferItem(entry, dst);
                }
            }
        }
        
        private static string GetHashDescription(CommandLineOptions options)
        {
            var msg =
                $"  MD5: {(options.AddMd5ToMetadata ? "enabled" : "disabled")}{(options.AddMd5ToMetadata ? $" and stored under {UploadHelper.KeyName} key" : string.Empty)}{Environment.NewLine}";
            return options.TransferType switch
            {
                TransferType.UploadDirectory => msg,
                TransferType.UploadFile => msg,
                TransferType.DownloadDirectory => string.Empty,
                TransferType.DownloadFile => string.Empty,
                _ => throw new ArgumentOutOfRangeException(nameof(options.TransferType))
            };
        }

        private static string GetTransferType(TransferType transferType)
        {
            return transferType switch
            {
                TransferType.UploadDirectory => "upload directory",
                TransferType.UploadFile => "upload file",
                TransferType.DownloadDirectory => "download directory",
                TransferType.DownloadFile => "download file",
                _ => throw new ArgumentOutOfRangeException(nameof(transferType))
            };
        }

        private static CommandLineOptions ParseArguments(IEnumerable<string> args)
        {
            var parserResult = Parser.Default.ParseArguments<CommandLineOptions>(args);
            if (parserResult.Tag == ParserResultType.NotParsed) return null;

            CommandLineOptions options = null;
            parserResult.WithParsed(o => options = o);
            return options;
        }

        private static void HookCancel(CancellationTokenSource cts)
        {
            void CancelEventHandler(object sender, ConsoleCancelEventArgs eventArgs)
            {
                Console.WriteLine("Canceling transfer...");

                cts.Cancel();
                eventArgs.Cancel = true;
                Console.CancelKeyPress -= CancelEventHandler;
                Console.CancelKeyPress += (_, __) =>
                {
                    Console.WriteLine("Terminating transfer...");
                    Environment.Exit(0);
                };
            }

            Console.CancelKeyPress += CancelEventHandler;
        }
    }
}