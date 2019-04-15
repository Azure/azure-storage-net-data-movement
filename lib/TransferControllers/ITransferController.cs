//------------------------------------------------------------------------------
// <copyright file="ITransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Threading.Tasks;

    internal interface ITransferController
    {
        bool HasWork
        {
            get;
        }

        bool IsFinished
        {
            get;
        }

        Task<bool> DoWorkAsync();

        void CancelWork();
    }
}
