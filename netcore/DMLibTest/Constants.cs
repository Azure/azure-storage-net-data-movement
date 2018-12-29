using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace DMLibTest
{
    internal static class Collections
    {
        public const string Global = "global";
    }

    internal static class RequestOptions
    {
        public static BlobRequestOptions DefaultBlobRequestOptions
        {
            get
            {
                return new BlobRequestOptions()
                {
                    MaximumExecutionTime = TimeSpan.FromMinutes(15)
                };
            }
        }
    }
}
