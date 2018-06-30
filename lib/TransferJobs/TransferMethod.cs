//------------------------------------------------------------------------------
// <copyright file="TransferMethod.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    internal enum TransferMethod
    {
        /// <summary>
        /// To read data from source to memory and then write the data in memory to destination.
        /// </summary>
        SyncCopy, 

        /// <summary>
        /// To send a start copy request to azure storage to let it do the copying,
        /// and monitor the copying progress until the copy finished.
        /// </summary>
        AsyncCopy,

        /// <summary>
        /// Creates dummy objects only, no data transfer will happen.
        /// </summary>
        DummyCopy,
    }
}
