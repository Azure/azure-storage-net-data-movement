using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Collections.Generic;

namespace DMLibTest
{
    public static class CloudBlobDirectoryExtensions
    {
        public static IEnumerable<IListBlobItem> ListBlobs(this CloudBlobDirectory dir, bool useFlatBlobListing = false, BlobListingDetails blobListingDetails = BlobListingDetails.None, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            // this is no longer a lazy method: the maximum (5000) results will be returned at once
            return dir.ListBlobsSegmentedAsync(useFlatBlobListing, blobListingDetails, maxResults: null, currentToken: null, options: options, operationContext: operationContext).GetAwaiter().GetResult().Results;
        }
    }
}
