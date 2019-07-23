//------------------------------------------------------------------------------
// <copyright file="CopyMethod.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Enum to indicate how the copying operation is handled in DataMovement Library.
    /// </summary>
    public enum CopyMethod
    {
        /// <summary>
        /// To read data from source to memory and then write the data in memory to destination.
        /// </summary>
        SyncCopy,

        /// <summary>
        /// To send a start copy request to azure storage to let it do the copying,
        /// and monitor the copying progress until the copy finished.
        /// </summary>
        ServiceSideAsyncCopy,

        /// <summary>
        /// To copy content of each chunk with with "Put Block From URL", "Append Block From URL" or "Put Page From URL".
        /// </summary>
        ServiceSideSyncCopy
    }
}
