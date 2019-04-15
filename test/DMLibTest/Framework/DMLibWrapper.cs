//------------------------------------------------------------------------------
// <copyright file="DMLibWrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public abstract class DMLibWrapper
    {
        public Task<TransferStatus> DoTransfer(TransferItem item)
        {
            Test.Info("Do transfer: {0}", item.ToString());
            return this.DoTransferImp(item);
        }

        protected abstract Task<TransferStatus> DoTransferImp(TransferItem item);
    }
}
