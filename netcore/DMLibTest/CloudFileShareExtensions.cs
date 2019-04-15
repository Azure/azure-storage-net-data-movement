using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;

namespace DMLibTest
{
    public static class CloudFileShareExtensions
    {
        public static void Create(this CloudFileShare share, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            share.CreateAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static bool CreateIfNotExists(this CloudFileShare share, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return share.CreateIfNotExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static void Delete(this CloudFileShare share, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            share.DeleteAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool DeleteIfExists(this CloudFileShare share, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            return share.DeleteIfExistsAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool Exists(this CloudFileShare share, FileRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return share.ExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static FileSharePermissions GetPermissions(this CloudFileShare share, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            return share.GetPermissionsAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static void SetPermissions(this CloudFileShare share, FileSharePermissions permissions, AccessCondition accessCondition = null, FileRequestOptions options = null, OperationContext operationContext = null)
        {
            share.SetPermissionsAsync(permissions, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
