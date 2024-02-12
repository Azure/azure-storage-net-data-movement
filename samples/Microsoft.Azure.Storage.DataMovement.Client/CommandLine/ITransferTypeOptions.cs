using Microsoft.Azure.Storage.DataMovement.Client.Transfers;

namespace Microsoft.Azure.Storage.DataMovement.Client.CommandLine
{
    internal interface ITransferTypeOptions
    {
        TransferType TransferType { get; }
    }
}