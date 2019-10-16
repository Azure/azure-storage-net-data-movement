//------------------------------------------------------------------------------
// <copyright file="BlockBlobDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    internal class BlockBlobDestHandler : BlobDestHandler
    {
        public BlockBlobDestHandler(AzureBlobLocation destLocation, TransferJob transferJob)
            : base(destLocation, transferJob)
        {
        }

        protected override Task CreateDestinationAsync(long length, AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
