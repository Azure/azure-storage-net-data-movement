//------------------------------------------------------------------------------
// <copyright file="StorageCopyState.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Extensions
{
    using System;
    using Microsoft.Azure.Storage.Blob;

    internal enum StorageCopyStatus
    {
        //
        // Summary:
        //     The copy status is invalid.
        Invalid = 0,
        //
        // Summary:
        //     The copy operation is pending.
        Pending = 1,
        //
        // Summary:
        //     The copy operation succeeded.
        Success = 2,
        //
        // Summary:
        //     The copy operation has been aborted.
        Aborted = 3,
        //
        // Summary:
        //     The copy operation encountered an error.
        Failed = 4
    }

    internal class StorageCopyState
    {
        public StorageCopyState(Microsoft.Azure.Storage.Blob.CopyState blobCopyState)
        {
            this.CopyId = blobCopyState.CopyId;
            this.SetStatus(blobCopyState.Status);
            this.Source = blobCopyState.Source;
            this.BytesCopied = blobCopyState.BytesCopied;
            this.TotalBytes = blobCopyState.TotalBytes;
            this.StatusDescription = blobCopyState.StatusDescription;
        }

        public StorageCopyState(Microsoft.Azure.Storage.File.CopyState fileCopyState)
        {
            this.CopyId = fileCopyState.CopyId;
            this.SetStatus(fileCopyState.Status);
            this.Source = fileCopyState.Source;
            this.BytesCopied = fileCopyState.BytesCopied;
            this.TotalBytes = fileCopyState.TotalBytes;
            this.StatusDescription = fileCopyState.StatusDescription;
        }

        private void SetStatus(Microsoft.Azure.Storage.Blob.CopyStatus blobCopyStatus)
        {
            switch (blobCopyStatus)
            {
                case Blob.CopyStatus.Invalid:
                    this.Status = StorageCopyStatus.Invalid;
                    break;
                case Blob.CopyStatus.Pending:
                    this.Status = StorageCopyStatus.Pending;
                    break;
                case Blob.CopyStatus.Success:
                    this.Status = StorageCopyStatus.Success;
                    break;
                case Blob.CopyStatus.Aborted:
                    this.Status = StorageCopyStatus.Aborted;
                    break;
                case Blob.CopyStatus.Failed:
                    this.Status = StorageCopyStatus.Failed;
                    break;
                default:
                    this.Status = StorageCopyStatus.Invalid;
                    break;
            }
        }

        private void SetStatus(Microsoft.Azure.Storage.File.CopyStatus fileCopyStatus)
        {
            switch (fileCopyStatus)
            {
                case File.CopyStatus.Invalid:
                    this.Status = StorageCopyStatus.Invalid;
                    break;
                case File.CopyStatus.Pending:
                    this.Status = StorageCopyStatus.Pending;
                    break;
                case File.CopyStatus.Success:
                    this.Status = StorageCopyStatus.Success;
                    break;
                case File.CopyStatus.Aborted:
                    this.Status = StorageCopyStatus.Aborted;
                    break;
                case File.CopyStatus.Failed:
                    this.Status = StorageCopyStatus.Failed;
                    break;
                default:
                    this.Status = StorageCopyStatus.Invalid;
                    break;
            }
        }

        //
        // Summary:
        //     Gets the ID of the copy operation.
        public string CopyId { get; private set; }
        //
        // Summary:
        //     Gets the status of the copy operation.
        public StorageCopyStatus Status { get; private set; }
        //
        // Summary:
        //     Gets the source URI of a copy operation.
        public Uri Source { get; private set; }
        //
        // Summary:
        //     Gets the number of bytes copied in the operation so far.
        public long? BytesCopied { get; set; }
        //
        // Summary:
        //     Gets the total number of bytes in the source of the copy.
        public long? TotalBytes { get; set; }
        //
        // Summary:
        //     Gets the description of the current status, if any.
        public string StatusDescription { get; private set; }
    }
}
