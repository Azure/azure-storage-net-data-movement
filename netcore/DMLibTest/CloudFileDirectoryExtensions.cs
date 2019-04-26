using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using System.Collections.Generic;
using System;
using System.Net;

namespace DMLibTest
{
    public static class CloudFileDirectoryExtensions
    {
        public static bool CreateIfNotExists(this CloudFileDirectory dir, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            try
            {
                return dir.CreateIfNotExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
            }
            catch (StorageException se)
            {
                // Creation against root directory throws 405 exception,
                // here swallow the error.
                if (null != se
                    && null != se.RequestInformation
                    && se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.MethodNotAllowed)
                {
                    return true;
                }
                else
                {
                    throw;
                }
            }
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
