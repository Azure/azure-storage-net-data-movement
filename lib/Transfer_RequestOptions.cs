//------------------------------------------------------------------------------
// <copyright file="Transfer_RequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.RetryPolicies;

    /// <summary>
    /// Defines default RequestOptions for every type of transfer job.
    /// </summary>
    internal static class Transfer_RequestOptions
    {
        /// <summary>
        /// Stores the default client retry count in x-ms error.
        /// </summary>
        private const int DefaultRetryCountXMsError = 10;

        /// <summary>
        /// Stores the default client retry count in non x-ms error.
        /// </summary>
        private const int DefaultRetryCountOtherError = 3;

        /// <summary>
        /// Stores the default maximum execution time across all potential retries. 
        /// </summary>
        private static readonly TimeSpan DefaultMaximumExecutionTime =
            TimeSpan.FromSeconds(900);

        /// <summary>
        /// Stores the default server timeout.
        /// </summary>
        private static readonly TimeSpan DefaultServerTimeout =
            TimeSpan.FromSeconds(300);

        public static readonly TimeSpan DefaultCreationServerTimeout =
            TimeSpan.FromSeconds(30);

        /// <summary>
        /// Stores the default back-off.
        /// Increases exponentially used with ExponentialRetry: 3, 9, 21, 45, 93, 120, 120, 120, ...
        /// </summary>
        private static readonly TimeSpan retryPoliciesDefaultBackoff =
            TimeSpan.FromSeconds(3.0);

        /// <summary>
        /// Gets the default <see cref="BlobRequestOptions"/>.
        /// </summary>
        /// <value>The default <see cref="BlobRequestOptions"/></value>
        public static BlobRequestOptions DefaultBlobRequestOptions
        {
            get
            {
                IRetryPolicy defaultRetryPolicy = new ExponentialRetry(
                    retryPoliciesDefaultBackoff, 
                    DefaultRetryCountXMsError);

                return new BlobRequestOptions()
                {
                    MaximumExecutionTime = DefaultMaximumExecutionTime,
                    RetryPolicy = defaultRetryPolicy,
                    ServerTimeout = DefaultServerTimeout,
                    UseTransactionalMD5 = true
                };
            }
        }

        /// <summary>
        /// Gets the default <see cref="BlobRequestOptions"/> for HTTPs transfers.
        /// </summary>
        /// <value>The default <see cref="BlobRequestOptions"/> for HTTPs transfers</value>
        public static BlobRequestOptions DefaultHttpsBlobRequestOptions
        {
            get
            {
                var defaultHttpsBlobRequestOptions = DefaultBlobRequestOptions;
                defaultHttpsBlobRequestOptions.UseTransactionalMD5 = false;

                return defaultHttpsBlobRequestOptions;
            }
        }

        /// <summary>
        /// Gets the default <see cref="FileRequestOptions"/>.
        /// </summary>
        /// <value>The default <see cref="FileRequestOptions"/></value>
        public static FileRequestOptions DefaultFileRequestOptions
        {
            get
            {
                IRetryPolicy defaultRetryPolicy = new ExponentialRetry(
                    retryPoliciesDefaultBackoff, 
                    DefaultRetryCountXMsError);

                return new FileRequestOptions()
                {
                    MaximumExecutionTime = DefaultMaximumExecutionTime,
                    RetryPolicy = defaultRetryPolicy,
                    ServerTimeout = DefaultServerTimeout,
                    UseTransactionalMD5 = true
                };
            }
        }

        /// <summary>
        /// Gets the default <see cref="FileRequestOptions"/> for HTTPs transfers.
        /// </summary>
        /// <value>The default <see cref="FileRequestOptions"/> for HTTPs transfers</value>
        public static FileRequestOptions DefaultHttpsFileRequestOptions
        {
            get
            {
                var defaultHttpsFileRequestOptions = DefaultFileRequestOptions;
                defaultHttpsFileRequestOptions.UseTransactionalMD5 = false;

                return defaultHttpsFileRequestOptions;
            }
        }

        /// <summary>
        /// Creates the default request options for specific location.
        /// </summary>
        /// <param name="location">The location <see cref="TransferLocation"/> which needs get a default request options.</param>
        /// <returns>The default request options which implements <see cref="IRequestOptions"/> for specific location.</returns>
        internal static IRequestOptions CreateDefaultRequestOptions(TransferLocation location)
        {
            if (null == location)
            {
                throw new ArgumentNullException(nameof(location));
            }

            IRequestOptions requestOptions;
            switch (location.Type)
            {
                case TransferLocationType.AzureBlob:
                    requestOptions = ((AzureBlobLocation)location).Blob.Uri.Scheme == Uri.UriSchemeHttps
                        ? DefaultHttpsBlobRequestOptions
                        : DefaultBlobRequestOptions;
                    break;
                case TransferLocationType.AzureBlobDirectory:
                    requestOptions = ((AzureBlobDirectoryLocation)location).BlobDirectory.Uri.Scheme == Uri.UriSchemeHttps
                        ? DefaultHttpsBlobRequestOptions
                        : DefaultBlobRequestOptions;
                    break;
                case TransferLocationType.AzureFile:
                    requestOptions = ((AzureFileLocation)location).AzureFile.Uri.Scheme == Uri.UriSchemeHttps
                        ? DefaultHttpsFileRequestOptions
                        : DefaultFileRequestOptions;
                    break;
                case TransferLocationType.AzureFileDirectory:
                    requestOptions = ((AzureFileDirectoryLocation)location).FileDirectory.Uri.Scheme == Uri.UriSchemeHttps
                        ? DefaultHttpsFileRequestOptions
                        : DefaultFileRequestOptions;
                    break;
                default:
                    throw new ArgumentException(
                        string.Format(
                            CultureInfo.CurrentCulture, 
                            "{0} is invalid, cannot get IRequestOptions for location type {1}", 
                            nameof(location), 
                            location.Type));
            }

            return requestOptions;
        }
    }
}
