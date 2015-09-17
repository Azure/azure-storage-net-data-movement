//------------------------------------------------------------------------------
// <copyright file="CopyWrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.DataMovement;

    internal class CopyWrapper : DMLibWrapper
    {
        public CopyWrapper()
        {
        }

        protected override Task DoTransferImp(TransferItem item)
        {
            return this.Copy(item.SourceObject, item.DestObject, item);
        }

        private Task Copy(dynamic sourceObject, dynamic destObject, TransferItem item)
        {
            CopyOptions copyOptions = item.Options as CopyOptions;
            TransferContext transferContext = item.TransferContext;
            CancellationToken cancellationToken = item.CancellationToken;

            if (cancellationToken != null && cancellationToken != CancellationToken.None)
            {
                return TransferManager.CopyAsync(sourceObject, destObject, item.IsServiceCopy, copyOptions, transferContext, cancellationToken);
            }
            else if (transferContext != null || copyOptions != null)
            {
                return TransferManager.CopyAsync(sourceObject, destObject, item.IsServiceCopy, copyOptions, transferContext);
            }
            else
            {
                return TransferManager.CopyAsync(sourceObject, destObject, item.IsServiceCopy);
            }
        }
    }
}
