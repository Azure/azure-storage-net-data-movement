//------------------------------------------------------------------------------
// <copyright file="CloudObjectExtensions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.RetryPolicies;

    internal static class CloudObjectExtensions
    {
        public static string GetShortName(this CloudBlob cloudBlob)
        {
            CloudBlobDirectory parentDir = cloudBlob.Parent;

            if (null == parentDir)
            {
                // Root directory
                return cloudBlob.Name;
            }

            return GetShortNameFromUri(cloudBlob.Uri.ToString(), parentDir.Uri.ToString());
        }

        public static string GetShortName(this CloudBlobDirectory cloudBlobDirectory)
        {
            CloudBlobDirectory parentDir = cloudBlobDirectory.Parent;

            if (null == parentDir)
            {
                // Root directory
                return String.Empty;
            }

            return GetShortNameFromUri(cloudBlobDirectory.Uri.ToString(), parentDir.Uri.ToString());
        }

        private static string GetShortNameFromUri(string uri, string parentUri)
        {
            string delimiter = "/";

            if (!parentUri.EndsWith(delimiter, StringComparison.Ordinal))
            {
                parentUri += delimiter;
            }

            string shortName = uri.Substring(parentUri.Length);

            if (shortName.EndsWith(delimiter, StringComparison.Ordinal))
            {
                shortName = shortName.Substring(0, shortName.Length - delimiter.Length);
            }

            return Uri.UnescapeDataString(shortName);
        }
    }

    internal static class HelperConst
    {
        public static BlobRequestOptions DefaultBlobOptions = new BlobRequestOptions
        {
            RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(90), 3),
            MaximumExecutionTime = TimeSpan.FromMinutes(15)
        };

        public static FileRequestOptions DefaultFileOptions = new FileRequestOptions
        {
            RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(90), 3),
            MaximumExecutionTime = TimeSpan.FromMinutes(15)
        };
    }
}
