using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Collections.Generic;

namespace DMLibTest
{
    public static class CloudBlobContainerExtensions
    {
        public static void Create(this CloudBlobContainer container, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            container.CreateAsync(BlobContainerPublicAccessType.Off, requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static bool CreateIfNotExists(this CloudBlobContainer container, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return container.CreateIfNotExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static void Delete(this CloudBlobContainer container, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            container.DeleteAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool DeleteIfExists(this CloudBlobContainer container, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return container.DeleteIfExistsAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static bool Exists(this CloudBlobContainer container, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return container.ExistsAsync(requestOptions, operationContext).GetAwaiter().GetResult();
        }

        public static BlobContainerPermissions GetPermissions(this CloudBlobContainer container, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return container.GetPermissionsAsync(accessCondition, options, operationContext).GetAwaiter().GetResult();
        }

        public static IEnumerable<IListBlobItem> ListBlobs(this CloudBlobContainer container, string prefix = null, bool useFlatBlobListing = false, BlobListingDetails blobListingDetails = BlobListingDetails.None, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            // this is no longer a lazy method: the maximum (5000) results will be returned at once
            return container.ListBlobsSegmentedAsync(prefix, useFlatBlobListing, blobListingDetails, null, null, options, operationContext).GetAwaiter().GetResult().Results;
        }

        public static void SetPermissions(this CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            container.SetPermissionsAsync(permissions, accessCondition, options, operationContext).GetAwaiter().GetResult();
        }
    }
}
