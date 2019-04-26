using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.IO;

namespace DMLibTest
{
    public static class CloudAppendBlobExtensions
    {
        public static void UploadFromByteArray(this CloudAppendBlob cloudBlob, byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            cloudBlob.UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromFile(this CloudAppendBlob cloudBlob, string path, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            cloudBlob.UploadFromFileAsync(path, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromStream(this CloudAppendBlob cloudBlob, Stream source, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            cloudBlob.UploadFromStreamAsync(source, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
