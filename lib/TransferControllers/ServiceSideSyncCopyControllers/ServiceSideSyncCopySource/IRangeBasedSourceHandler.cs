//------------------------------------------------------------------------------
// <copyright file="IRangeBasedSourceHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopySource
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    interface IRangeBasedSourceHandler : ISourceHandler
    {
        Task<List<Utils.Range>> GetCopyRangesAsync(long startOffset, long length, CancellationToken cancellationToken);
    }
}
