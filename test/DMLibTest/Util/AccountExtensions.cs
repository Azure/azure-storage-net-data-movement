using System;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.File;

namespace DMLibTest
{
    internal static class AccountExtensions
    {
        public static CloudFileClient CreateCloudFileClient(CloudStorageAccount account)
        {
            if (account.FileEndpoint == null)
                throw new InvalidOperationException("No file endpoint configured.");
            return new CloudFileClient(account.FileStorageUri, account.Credentials, new SecurityProtocolHandler());
        }

        public static CloudBlobClient CreateCloudBlobClient(CloudStorageAccount account)
        {
            if (account.BlobEndpoint == null)
                throw new InvalidOperationException("No blob endpoint configured.");
            return new CloudBlobClient(account.BlobStorageUri, account.Credentials, new SecurityProtocolHandler());
        }
    }
}