using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal interface ITransfer
    {
        string JobId { get; }
        Task<TransferStatus> ExecuteAsync(CancellationToken token);
    }
}