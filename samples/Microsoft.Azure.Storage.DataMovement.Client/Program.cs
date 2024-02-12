using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Azure.Storage.DataMovement.Client.CommandLine;
using Microsoft.Azure.Storage.DataMovement.Client.Logger;
using Microsoft.Azure.Storage.DataMovement.Client.Transfers;

namespace Microsoft.Azure.Storage.DataMovement.Client
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            await ParseArguments(args);
        }

        private static async Task TransferListOfItems(ListOfItemsCommandLineOptions options)
        {
            Console.WriteLine("Starting list of items transfer:");
            Console.WriteLine($"  from: {options.Source}");
            Console.WriteLine($"DMLib version: {typeof(TransferManager).Assembly.GetName().Version}{Environment.NewLine}");

            using var cts = new CancellationTokenSource();
            HookCancel(cts);
            var transferFactory = new TransferFactory(options);
            var elapsed = new Stopwatch();
            elapsed.Start();

            var transfer = transferFactory.Create();
            var result = await transfer.ExecuteAsync(cts.Token).ConfigureAwait(false);

            result.PrintResult(transfer.JobId);

            elapsed.Stop();

            Console.WriteLine($"{Environment.NewLine}The transfer ended up. Transfer took {elapsed.Elapsed.TotalSeconds:0} seconds.");
        }

        private static async Task TransferDefault(CommandLineOptions options)
        {
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
                var transfer = transferFactory.Create();
                var result = await transfer.ExecuteAsync(cts.Token).ConfigureAwait(false);

                result.PrintResult(transfer.JobId);
            });

            await Task.WhenAll(tasks).ConfigureAwait(false);

            elapsed.Stop();

            Console.WriteLine(
                $"{Environment.NewLine}All transfers ended up. Transfers took {elapsed.Elapsed.TotalSeconds:0} seconds");
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

        private static async Task ParseArguments(IEnumerable<string> args)
        {
            var parserResult = Parser.Default.ParseArguments<CommandLineOptions, ListOfItemsCommandLineOptions>(args);
            if (parserResult.Tag == ParserResultType.NotParsed) return;

            await parserResult.MapResult(
                (CommandLineOptions options) => TransferDefault(options),
                (ListOfItemsCommandLineOptions options) => TransferListOfItems(options),
                errors => Task.CompletedTask); ;
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