using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Collections.Generic;
using System.IO;

namespace DMLibTest
{
    public static class CloudPageBlobExtensions
    {
        public static void Create(this CloudPageBlob blob, long size, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.CreateAsync(size, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static IEnumerable<PageRange> GetPageRanges(this CloudPageBlob blob, long? offset = default(long?), long? length = default(long?), AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return blob.GetPageRangesAsync(offset, length, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromByteArray(this CloudPageBlob blob, byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromFile(this CloudPageBlob blob, string path, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromFileAsync(path, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromStream(this CloudPageBlob blob, Stream source, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.UploadFromStreamAsync(source, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void WritePages(this CloudPageBlob blob, Stream pageData, long startOffset, string contentMD5 = null, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            blob.WritePagesAsync(pageData, startOffset, contentMD5).GetAwaiter().GetResult();
        }
    }
}
