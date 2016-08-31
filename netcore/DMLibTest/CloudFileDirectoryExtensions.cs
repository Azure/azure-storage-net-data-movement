using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using System.Collections.Generic;

namespace DMLibTest
{
    public static class CloudFileDirectoryExtensions
    {
        public static bool CreateIfNotExists(this CloudFileDirectory dir, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return dir.CreateIfNotExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static void Delete(this CloudFileDirectory dir, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            dir.DeleteAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool Exists(this CloudFileDirectory dir, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return dir.ExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static IEnumerable<IListFileItem> ListFilesAndDirectories(this CloudFileDirectory dir, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            FileContinuationToken continuationToken = new FileContinuationToken();

            // this is no longer a lazy method: ListFilesAndDirectoriesSegmentedAsync will return the maximum results (5000) at once
            return dir.ListFilesAndDirectoriesSegmentedAsync(null, continuationToken, options, operationContext).GetAwaiter().GetResult().Results;
        }
    }
}
