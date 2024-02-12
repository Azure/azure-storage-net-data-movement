//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

using System.Linq;
using Microsoft.Azure.Storage.DataMovement.Dto;

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Threading;

    internal class DirectEnumerator : ITransferEnumerator
    {
        private readonly IEnumerable<TransferItem> transferItems;

        public DirectEnumerator(IEnumerable<TransferItem> transferItems)
        {
            this.transferItems = transferItems;
        }

        public ListContinuationToken EnumerateContinuationToken { get; set; }
        public IEnumerable<TransferEntry> EnumerateLocation(CancellationToken cancellationToken)
        {
            var token = EnumerateContinuationToken as DirectEnumerationContinuationToken;
            return transferItems
                .Skip(token?.SkipCount ?? 0)
                .Select((item, idx) =>
                    new FileEntry(item.Destination, item.Source, new DirectEnumerationContinuationToken(idx + 1)));
        }
    }
}
