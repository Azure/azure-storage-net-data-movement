using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using System.Collections.Generic;
using System.IO;

namespace DMLibTest
{
    public static class CloudFileExtensions
    {
        public static void Create(this CloudFile file, long size, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.CreateAsync(size, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void Delete(this CloudFile file, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.DeleteAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool DeleteIfExists(this CloudFile file, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            return file.DeleteIfExistsAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void DownloadToFile(this CloudFile file, string path, FileMode mode, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.DownloadToFileAsync(path, mode, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void DownloadToStream(this CloudFile file, Stream target, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.DownloadToStreamAsync(target, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool Exists(this CloudFile file, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            return file.ExistsAsync(options, operationContext).GetAwaiter().GetResult();
        }

        public static void FetchAttributes(this CloudFile file, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.FetchAttributesAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static IEnumerable<FileRange> ListRanges(this CloudFile file, long? offset = default(long?), long? length = default(long?), AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            return file.ListRangesAsync(offset, length, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void SetMetadata(this CloudFile file, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.SetMetadataAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void SetProperties(this CloudFile file, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.SetPropertiesAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromFile(this CloudFile file, string path, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.UploadFromFileAsync(path, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void UploadFromStream(this CloudFile file, Stream source, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.UploadFromStreamAsync(source, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void WriteRange(this CloudFile file, Stream rangeData, long startOffset, string contentMD5 = null, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            file.WriteRangeAsync(rangeData, startOffset, contentMD5, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
