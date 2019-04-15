using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Collections.Generic;
using System.IO;

namespace DMLibTest
{
    public static class CloudBlockBlobExtensions
    {
        public static IEnumerable<ListBlockItem> DownloadBlockList(this CloudBlockBlob blob, BlockListingFilter blockListingFilter = BlockListingFilter.Committed, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.DownloadBlockListAsync(blockListingFilter, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void PutBlock(this CloudBlockBlob blob, string blockId, Stream blockData, string contentMD5, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.PutBlockAsync(blockId, blockData, contentMD5, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void PutBlockList(this CloudBlockBlob blob, IEnumerable<string> blockList, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.PutBlockListAsync(blockList, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromByteArray(this CloudBlockBlob blob, byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromFile(this CloudBlockBlob blob, string path, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromFileAsync(path, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromStream(this CloudBlockBlob blob, Stream source, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromStreamAsync(source, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
