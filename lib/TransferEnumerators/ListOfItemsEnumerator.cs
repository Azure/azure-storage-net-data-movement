using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Azure.Storage.DataMovement.Dto;

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    internal class ListOfItemsEnumerator : ITransferEnumerator
    {
        private readonly IEnumerable<TransferItem> transferItems;
        private readonly IDataMovementLogger logger;

        public ListOfItemsEnumerator(IEnumerable<TransferItem> transferItems, IDataMovementLogger logger)
        {
            this.transferItems = transferItems;
            this.logger = logger;
        }

        public ListContinuationToken EnumerateContinuationToken { get; set; }

        public IEnumerable<TransferEntry> EnumerateLocation(CancellationToken cancellationToken)
        {
            var token = EnumerateContinuationToken as ListOfItemsEnumerationContinuationToken;
            logger.Info($"Continuation token skip count value is {token?.SkipCount ?? 0}");

            return transferItems
                .Skip(token?.SkipCount ?? 0)
                .Select((item, idx) =>
                    new FileEntry(item.Destination, item.Source, new ListOfItemsEnumerationContinuationToken(idx + 1)));
        }
    }
}