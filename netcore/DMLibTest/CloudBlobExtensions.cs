using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;

namespace DMLibTest
{
    public static class CloudBlobExtensions
    {
        public static string AcquireLease(this CloudBlob blob, TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.AcquireLeaseAsync(leaseTime, proposedLeaseId, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void Delete(this CloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.DeleteAsync(deleteSnapshotsOption, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool DeleteIfExists(this CloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.DeleteIfExistsAsync(deleteSnapshotsOption, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void DownloadToFile(this CloudBlob blob, string path, FileMode mode, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.DownloadToFileAsync(path, mode, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void DownloadToStream(this CloudBlob blob, Stream target, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.DownloadToStreamAsync(target, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool Exists(this CloudBlob blob, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.ExistsAsync(options, operationContext).GetAwaiter().GetResult();
        }

        public static void FetchAttributes(this CloudBlob blob, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.FetchAttributesAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void ReleaseLease(this CloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.ReleaseLeaseAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void SetMetadata(this CloudBlob blob, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.SetMetadataAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void SetProperties(this CloudBlob blob, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.SetPropertiesAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static CloudBlob Snapshot(this CloudBlob blob, IDictionary<string, string> metadata = null, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.SnapshotAsync(metadata, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
