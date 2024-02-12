using CommandLine;
using Microsoft.Azure.Storage.DataMovement.Client.Transfers;

namespace Microsoft.Azure.Storage.DataMovement.Client.CommandLine
{
    [Verb("transfer-list", HelpText = "Allow transfer list of items defined in a load file.")]
    internal class ListOfItemsCommandLineOptions : ILoggerConfiguration, ITransferTypeOptions
    {
        [Option('l', "load-file-path", HelpText = "A load file path.", Required = true)]
        public string Source { get; set; }
        
        [Option('d', "destination", HelpText = "Destination path of item to be transferred.", Required = true)]
        public string Destination { get; set; }        

        [Option('t', "sas-token", HelpText = "SAS token.", Required = true)]
        public string SasToken { get; set; }

        [Option("console-logger", HelpText = "Add console logger.", Required = false, Default = false)]
        public bool AddConsoleLogger { get; set; }

        public TransferType TransferType => TransferType.ListOfItems;
    }
}