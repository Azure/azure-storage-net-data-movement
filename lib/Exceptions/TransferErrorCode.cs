//------------------------------------------------------------------------------
// <copyright file="TransferErrorCode.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;

    /// <summary>
    /// Error codes for TransferException.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1027:MarkEnumsWithFlags")]
    public enum TransferErrorCode
    {
        /// <summary>
        /// No error.
        /// </summary>
        None = 0,

        /// <summary>
        /// Failed to open file for upload or download.
        /// </summary>
        OpenFileFailed = 3,

        /// <summary>
        /// The file to transfer is too large for the destination.
        /// </summary>
        UploadSourceFileSizeTooLarge = 4,

        /// <summary>
        /// The file size is invalid for the specified blob type.
        /// </summary>
        UploadBlobSourceFileSizeInvalid = 5,

        /// <summary>
        /// User canceled.
        /// </summary>
        OperationCanceled = 6,

        /// <summary>
        /// Both Source and Destination are locally accessible locations.
        /// At least one of source and destination should be an Azure Storage location.
        /// </summary>
        LocalToLocalTransfersUnsupported = 7,

        /// <summary>
        /// Failed to do asynchronous copy.
        /// </summary>
        AsyncCopyFailed = 8,

        /// <summary>
        /// Source and destination are the same.
        /// </summary>
        SameSourceAndDestination = 9,

        /// <summary>
        /// AsyncCopyController detects mismatch between copy id stored in transfer entry and 
        /// that retrieved from server.
        /// </summary>
        MismatchCopyId = 10,

        /// <summary>
        /// AsyncCopyControler fails to retrieve CopyState for the object which we are to monitor.
        /// </summary>
        FailToRetrieveCopyStateForObject = 11,

        /// <summary>
        /// Fails to allocate memory in MemoryManager.
        /// </summary>
        FailToAllocateMemory = 12,

        /// <summary>
        /// Fails to get source's last write time.
        /// </summary>
        FailToGetSourceLastWriteTime = 13,

        /// <summary>
        /// User choose not to overwrite existing destination.
        /// </summary>
        NotOverwriteExistingDestination = 14,

        /// <summary>
        /// Transfer with the same source and destination already exists.
        /// </summary>
        TransferAlreadyExists = 15,

        /// <summary>
        /// Fails to enumerate directory.
        /// </summary>
        FailToEnumerateDirectory = 16,

        /// <summary>
        /// Fails to validate destination.
        /// </summary>
        FailToVadlidateDestination = 17,

        /// <summary>
        /// Sub transfer fails.
        /// </summary>
        SubTransferFails = 18,

        /// <summary>
        /// The source file size is invalid for azure file.
        /// </summary>
        UploadFileSourceFileSizeInvalid = 19,

        /// <summary>
        /// Failed to create directory because a file already exists with the same name.
        /// </summary>
        FailedToCreateDirectory = 20,

        /// <summary>
        /// The transfer type didn't support dummy transfer.
        /// </summary>
        UnsupportedDummyTransfer = 21,

        /// <summary>
        /// Uncategorized transfer error.
        /// </summary>
        Unknown = 32,
    }
}
