﻿using System;
using System.Collections.Concurrent;
using System.Linq;
using CommandLine;
using Microsoft.Azure.Storage.DataMovement.Client.Transfers;

namespace Microsoft.Azure.Storage.DataMovement.Client.CommandLine
{
    [Verb("transfer", true, HelpText = "Allow transfer a file or directory to or from ADLS.")]
    internal class CommandLineOptions : IBaseOptions, ITransferTypeOptions
    {
        private readonly ConcurrentQueue<string> sasTokensQueue = new();
        private string sasTokens;

        [Option('s', "source", HelpText = "Source path of item to be transferred.", Required = true)]
        public string Source { get; set; }

        [Option('d', "destination", HelpText = "Destination path of item to be transferred.", Required = true)]
        public string Destination { get; set; }

        [Option('n', "transfers-number", HelpText = "Number of simultaneous transfers", Required = false, Default = 1)]
        public int TransfersNumber { get; set; }

        [Option('t', "sas-tokens", HelpText = "Comma separated SAS tokens (Should be >= -n parameter).",
            Required = true)]
        public string SasTokens
        {
            set
            {
                sasTokens = value;

                foreach (var sasToken in (sasTokens ?? string.Empty).Split(',')
                         .Select(x => x.Trim())
                         .Where(x => !string.IsNullOrWhiteSpace(x)))
                    sasTokensQueue.Enqueue(sasToken);
            }
        }

        [Option('r', "transfer-type",
            HelpText =
                "Transfer type (Possible values are: UploadDirectory, UploadFile, DownloadDirectory, DownloadFile).",
            Required = false, Default = TransferType.UploadDirectory)]
        public TransferType TransferType { get; set; }

        [Option("store-hash", HelpText = "Stores additional file's hash in metadata (Not supported by download).",
            Required = false, Default = false)]
        public bool AddMd5ToMetadata { get; set; }

        [Option("console-logger", HelpText = "Add console logger.", Required = false, Default = false)]
        public bool AddConsoleLogger { get; set; }

        internal string GetSasToken()
        {
            if (sasTokensQueue.Count == 0)
                throw new Exception("No more SAS tokens available. Provide more with -t parameter.");

            sasTokensQueue.TryDequeue(out var sasToken);

            return sasToken;
        }
        
        [Option('j', "jobId", HelpText = "Job id.", Required = false)]
        public Guid? JobId { get; set; }
    }
}