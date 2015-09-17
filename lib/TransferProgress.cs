//-----------------------------------------------------------------------------
// <copyright file="TransferProgress.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    /// <summary>
    /// Transfer progress
    /// </summary>
    public sealed class TransferProgress
    {
        /// <summary>
        /// Gets the number of bytes that have been transferred.
        /// </summary>
        public long BytesTransferred
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the number of files that have been transferred.
        /// </summary>
        public long NumberOfFilesTransferred
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the number of files that are skipped to be transferred.
        /// </summary>
        public long NumberOfFilesSkipped
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the number of files that are failed to be transferred.
        /// </summary>
        public long NumberOfFilesFailed
        {
            get;
            internal set;
        }
    }
}
