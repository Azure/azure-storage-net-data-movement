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
        /// To download data from source to memory, and upload the data from memory to destination.
        /// </summary>
        SyncCopy,

        /// <summary>
        /// To send a start copy request to azure storage to let it do the copying,
        /// and monitor the copying progress until the copy completed.
        /// </summary>
        ServiceSideAsyncCopy,

        /// <summary>
        /// To copy content of each chunk with with Put Block From URL, Append Block From URL or Put Page From URL.
        /// See <c>https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url</c> for Put Block From URL,
        /// <c>https://docs.microsoft.com/en-us/rest/api/storageservices/append-block-from-url</c> for Append Block From URL,
        /// <c>https://docs.microsoft.com/en-us/rest/api/storageservices/put-page-from-url</c> for Put Page From URL for details.
        /// </summary>
        ServiceSideSyncCopy
    }
}
