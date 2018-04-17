//------------------------------------------------------------------------------
// <copyright file="Transfer_RequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Table;

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
                IRetryPolicy defaultRetryPolicy = new TransferRetryPolicy(
                    retryPoliciesDefaultBackoff,
                    DefaultRetryCountXMsError,
                    DefaultRetryCountOtherError);

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
                IRetryPolicy defaultRetryPolicy = new TransferRetryPolicy(
                    retryPoliciesDefaultBackoff,
                    DefaultRetryCountXMsError,
                    DefaultRetryCountOtherError);

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

        /// <summary>
        /// Gets the default <see cref="TableRequestOptions"/>.
        /// </summary>
        /// <value>The default <see cref="TableRequestOptions"/></value>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "It will be called in TableDataMovement project.")]
        public static TableRequestOptions DefaultTableRequestOptions
        {
            get
            {
                IRetryPolicy defaultRetryPolicy = new TransferRetryPolicy(
                    retryPoliciesDefaultBackoff,
                    DefaultRetryCountXMsError,
                    DefaultRetryCountOtherError);

                return new TableRequestOptions
                {
                    MaximumExecutionTime = DefaultMaximumExecutionTime,
                    RetryPolicy = defaultRetryPolicy,
                    ServerTimeout = DefaultServerTimeout,
                    PayloadFormat = TablePayloadFormat.Json
                };
            }
        }

        /// <summary>
        /// Define retry policy used in blob transfer.
        /// </summary>
        private class TransferRetryPolicy : IExtendedRetryPolicy
        {
            /// <summary>
            /// Prefix of Azure Storage response keys.
            /// </summary>
            private const string XMsPrefix = "x-ms";

            /// <summary>
            /// Max retry count in non x-ms error.
            /// </summary>
            private readonly int maxAttemptsOtherError;

            /// <summary>
            /// ExponentialRetry retry policy object.
            /// </summary>
            private readonly ExponentialRetry retryPolicy;

#if !DOTNET5_4
            /// <summary>
            /// Indicate whether has met x-ms once or more.
            /// </summary>
            private bool gotXMsError = false;
#endif

            /// <summary>
            /// Initializes a new instance of the <see cref="TransferRetryPolicy"/> class.
            /// </summary>
            /// <param name="deltaBackoff">Back-off in ExponentialRetry retry policy.</param>
            /// <param name="maxAttemptsXMsError">Max retry count when meets x-ms error.</param>
            /// <param name="maxAttemptsOtherError">Max retry count when meets non x-ms error.</param>
            public TransferRetryPolicy(TimeSpan deltaBackoff, int maxAttemptsXMsError, int maxAttemptsOtherError)
            {
                Debug.Assert(
                    maxAttemptsXMsError >= maxAttemptsOtherError,
                    "We should retry more times when meets x-ms errors than the other errors.");

                this.retryPolicy = new ExponentialRetry(deltaBackoff, maxAttemptsXMsError);
                this.maxAttemptsOtherError = maxAttemptsOtherError;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="TransferRetryPolicy"/> class.
            /// </summary>
            /// <param name="retryPolicy">ExponentialRetry object.</param>
            /// <param name="maxAttemptsInOtherError">Max retry count when meets non x-ms error.</param>
            private TransferRetryPolicy(ExponentialRetry retryPolicy, int maxAttemptsInOtherError)
            {
                this.retryPolicy = retryPolicy;
                this.maxAttemptsOtherError = maxAttemptsInOtherError;
            }

            /// <summary>
            /// Generates a new retry policy for the current request attempt.
            /// </summary>
            /// <returns>An IRetryPolicy object that represents the retry policy for the current request attempt.</returns>
            public IRetryPolicy CreateInstance()
            {
                return new TransferRetryPolicy(
                    this.retryPolicy.CreateInstance() as ExponentialRetry,
                    this.maxAttemptsOtherError);
            }

            /// <summary>
            /// Determines whether the operation should be retried and the interval until the next retry.
            /// </summary>
            /// <param name="retryContext">
            /// A RetryContext object that indicates the number of retries, the results of the last request, 
            /// and whether the next retry should happen in the primary or secondary location, and specifies the location mode.</param>
            /// <param name="operationContext">An OperationContext object for tracking the current operation.</param>
            /// <returns>
            /// A RetryInfo object that indicates the location mode, 
            /// and whether the next retry should happen in the primary or secondary location. 
            /// If null, the operation will not be retried. </returns>
            public RetryInfo Evaluate(RetryContext retryContext, OperationContext operationContext)
            {
                if (null == retryContext)
                {
                    throw new ArgumentNullException(nameof(retryContext));
                }

                if (null == operationContext)
                {
                    throw new ArgumentNullException(nameof(operationContext));
                }

                RetryInfo retryInfo = this.retryPolicy.Evaluate(retryContext, operationContext);

                if (null != retryInfo)
                {
                    if (this.ShouldRetry(retryContext.CurrentRetryCount, retryContext.LastRequestResult.Exception))
                    {
                        return retryInfo;
                    }
                }

                return null;
            }

            /// <summary>
            /// Determines if the operation should be retried and how long to wait until the next retry.
            /// </summary>
            /// <param name="currentRetryCount">The number of retries for the given operation.</param>
            /// <param name="statusCode">The status code for the last operation.</param>
            /// <param name="lastException">An Exception object that represents the last exception encountered.</param>
            /// <param name="retryInterval">The interval to wait until the next retry.</param>
            /// <param name="operationContext">An OperationContext object for tracking the current operation.</param>
            /// <returns>True if the operation should be retried; otherwise, false.</returns>
            public bool ShouldRetry(
                int currentRetryCount,
                int statusCode,
                Exception lastException,
                out TimeSpan retryInterval,
                OperationContext operationContext)
            {
                if (!this.retryPolicy.ShouldRetry(currentRetryCount, statusCode, lastException, out retryInterval, operationContext))
                {
                    return false;
                }

                return this.ShouldRetry(currentRetryCount, lastException);
            }

            /// <summary>
            /// Determines if the operation should be retried.
            /// This function uses http header to determine whether the error is returned from Windows Azure.
            /// If it's from Windows Azure (with <c>x-ms</c> in header), the request will retry 10 times at most.
            /// Otherwise, the request will retry 3 times at most.
            /// </summary>
            /// <param name="currentRetryCount">The number of retries for the given operation.</param>
            /// <param name="lastException">An Exception object that represents the last exception encountered.</param>
            /// <returns>True if the operation should be retried; otherwise, false.</returns>
            private bool ShouldRetry(
                int currentRetryCount,
                Exception lastException)
            {
#if DOTNET5_4
                return true;
#else
                if (this.gotXMsError)
                {
                    return true;
                }

                StorageException storageException = lastException as StorageException;

                if (null != storageException)
                {
                    WebException webException = storageException.InnerException as WebException;

                    if (null != webException)
                    {
                        if (WebExceptionStatus.ConnectionClosed == webException.Status)
                        {
                            return true;
                        }

                        HttpWebResponse response = webException.Response as HttpWebResponse;

                        if (null != response)
                        {
                            if (null != response.Headers)
                            {
                                if (null != response.Headers.AllKeys)
                                {
                                    for (int i = 0; i < response.Headers.AllKeys.Length; ++i)
                                    {
                                        if (response.Headers.AllKeys[i].StartsWith(XMsPrefix, StringComparison.OrdinalIgnoreCase))
                                        {
                                            this.gotXMsError = true;
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (currentRetryCount < this.maxAttemptsOtherError)
                {
                    return true;
                }

                return false;
#endif
            }
        }
    }
}
