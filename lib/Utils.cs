//------------------------------------------------------------------------------
// <copyright file="Utils.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Class for various utils.
    /// </summary>
    internal static class Utils
    {
        private const int RequireBufferMaxRetryCount = 10;

        /// <summary>
        /// Define the various possible size postfixes.
        /// </summary>
        private static readonly string[] SizeFormats = 
        {
            Resources.ReadableSizeFormatBytes, 
            Resources.ReadableSizeFormatKiloBytes, 
            Resources.ReadableSizeFormatMegaBytes, 
            Resources.ReadableSizeFormatGigaBytes, 
            Resources.ReadableSizeFormatTeraBytes, 
            Resources.ReadableSizeFormatPetaBytes, 
            Resources.ReadableSizeFormatExaBytes 
        };

        /// <summary>
        /// Translate a size in bytes to human readable form.
        /// </summary>
        /// <param name="size">Size in bytes.</param>
        /// <returns>Human readable form string.</returns>
        public static string BytesToHumanReadableSize(double size)
        {
            int order = 0;

            while (size >= 1024 && order + 1 < SizeFormats.Length)
            {
                ++order;
                size /= 1024;
            }

            return string.Format(CultureInfo.CurrentCulture, SizeFormats[order], size);
        }

        public static void CheckCancellation(CancellationTokenSource cancellationTokenSource)
        {
            if (cancellationTokenSource.IsCancellationRequested)
            {
                throw new OperationCanceledException(Resources.BlobTransferCancelledException);
            }
        }

        /// <summary>
        /// Generate an AccessCondition instance of IfMatchETag with customer condition.
        /// For download/copy, if it succeeded at the first operation to fetching attribute with customer condition,
        /// it means that the blob totally meet the condition. 
        /// Here, only need to keep LeaseId in the customer condition for the following operations.
        /// </summary>
        /// <param name="etag">ETag string.</param>
        /// <param name="customCondition">Condition customer input in TransferLocation.</param>
        /// <param name="checkedCustomAC">To specify whether have already verified the custom access condition against the blob.</param>
        /// <returns>AccessCondition instance of IfMatchETag with customer condition's LeaseId.</returns>
        public static AccessCondition GenerateIfMatchConditionWithCustomerCondition(
            string etag,
            AccessCondition customCondition,
            bool checkedCustomAC = true)
        {
            if (!checkedCustomAC)
            {
                return customCondition;
            }

            AccessCondition accessCondition = AccessCondition.GenerateIfMatchCondition(etag);

            if (null != customCondition)
            {
                accessCondition.LeaseId = customCondition.LeaseId;
            }

            return accessCondition;
        }

        public static bool DictionaryEquals(
            this IDictionary<string, string> firstDic, IDictionary<string, string> secondDic)
        {
            if (firstDic == secondDic)
            {
                return true;
            }

            if (firstDic == null || secondDic == null)
            {
                return false;
            }

            if (firstDic.Count != secondDic.Count)
            {
                return false;
            }

            foreach (var pair in firstDic)
            {
                string secondValue;
                if (!secondDic.TryGetValue(pair.Key, out secondValue))
                {
                    return false;
                }

                if (!string.Equals(pair.Value, secondValue, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public static Attributes GenerateAttributes(CloudBlob blob)
        {
            return new Attributes()
            {
                CacheControl = blob.Properties.CacheControl,
                ContentDisposition = blob.Properties.ContentDisposition,
                ContentEncoding = blob.Properties.ContentEncoding,
                ContentLanguage = blob.Properties.ContentLanguage,
                ContentMD5 = blob.Properties.ContentMD5,
                ContentType = blob.Properties.ContentType,
                Metadata = blob.Metadata,
                OverWriteAll = true
            };
        }

        public static Attributes GenerateAttributes(CloudFile file)
        {
            return new Attributes()
            {
                CacheControl = file.Properties.CacheControl,
                ContentDisposition = file.Properties.ContentDisposition,
                ContentEncoding = file.Properties.ContentEncoding,
                ContentLanguage = file.Properties.ContentLanguage,
                ContentMD5 = file.Properties.ContentMD5,
                ContentType = file.Properties.ContentType,
                Metadata = file.Metadata,
                OverWriteAll = true
            };
        }

        public static void SetAttributes(CloudBlob blob, Attributes attributes)
        {
            if (attributes.OverWriteAll)
            {
                blob.Properties.CacheControl = attributes.CacheControl;
                blob.Properties.ContentDisposition = attributes.ContentDisposition;
                blob.Properties.ContentEncoding = attributes.ContentEncoding;
                blob.Properties.ContentLanguage = attributes.ContentLanguage;
                blob.Properties.ContentMD5 = attributes.ContentMD5;
                blob.Properties.ContentType = attributes.ContentType;

                blob.Metadata.Clear();

                foreach (var metadataPair in attributes.Metadata)
                {
                    blob.Metadata.Add(metadataPair);
                }
            }
            else
            {
                blob.Properties.ContentMD5 = attributes.ContentMD5;
                if (null != attributes.ContentType)
                {
                    blob.Properties.ContentType = attributes.ContentType;
                }
            }
        }

        public static void SetAttributes(CloudFile file, Attributes attributes)
        {
            if (attributes.OverWriteAll)
            {
                file.Properties.CacheControl = attributes.CacheControl;
                file.Properties.ContentDisposition = attributes.ContentDisposition;
                file.Properties.ContentEncoding = attributes.ContentEncoding;
                file.Properties.ContentLanguage = attributes.ContentLanguage;
                file.Properties.ContentMD5 = attributes.ContentMD5;
                file.Properties.ContentType = attributes.ContentType;

                file.Metadata.Clear();

                foreach (var metadataPair in attributes.Metadata)
                {
                    file.Metadata.Add(metadataPair);
                }
            }
            else
            {
                file.Properties.ContentMD5 = attributes.ContentMD5;

                if (null != attributes.ContentType)
                {
                    file.Properties.ContentType = attributes.ContentType;
                }
            }
        }

        /// <summary>
        /// Generate an AccessCondition instance with lease id customer condition.
        /// For upload/copy, if it succeeded at the first operation to fetching destination attribute with customer condition,
        /// it means that the blob totally meet the condition. 
        /// Here, only need to keep LeaseId in the customer condition for the following operations.
        /// </summary>
        /// <param name="customCondition">Condition customer input in TransferLocation.</param>
        /// <param name="checkedCustomAC">To specify whether have already verified the custom access condition against the blob.</param>
        /// <returns>AccessCondition instance with customer condition's LeaseId.</returns>
        public static AccessCondition GenerateConditionWithCustomerCondition(
            AccessCondition customCondition,
            bool checkedCustomAC = true)
        {
            if (!checkedCustomAC)
            {
                return customCondition;
            }

            if ((null != customCondition)
                && !string.IsNullOrEmpty(customCondition.LeaseId))
            {
                return AccessCondition.GenerateLeaseCondition(customCondition.LeaseId);
            }

            return null;
        }

        /// <summary>
        /// Generate a BlobRequestOptions with custom BlobRequestOptions.
        /// We have default MaximumExecutionTime, ServerTimeout and RetryPolicy. 
        /// If user doesn't set these properties, we should use the default ones.
        /// Others, we should the custom ones.
        /// </summary>
        /// <param name="customRequestOptions">BlobRequestOptions customer input in TransferLocation.</param>
        /// <returns>BlobRequestOptions instance with custom BlobRequestOptions properties.</returns>
        public static BlobRequestOptions GenerateBlobRequestOptions(
            BlobRequestOptions customRequestOptions)
        {
            if (null == customRequestOptions)
            {
                return Transfer_RequestOptions.DefaultBlobRequestOptions;
            }
            else
            {
                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;

                AssignToRequestOptions(requestOptions, customRequestOptions);

                if (null != customRequestOptions.UseTransactionalMD5)
                {
                    requestOptions.UseTransactionalMD5 = customRequestOptions.UseTransactionalMD5;
                }

                requestOptions.DisableContentMD5Validation = customRequestOptions.DisableContentMD5Validation;
                return requestOptions;
            }
        }

        /// <summary>
        /// Generate a FileRequestOptions with custom FileRequestOptions.
        /// We have default MaximumExecutionTime, ServerTimeout and RetryPolicy. 
        /// If user doesn't set these properties, we should use the default ones.
        /// Others, we should the custom ones.
        /// </summary>
        /// <param name="customRequestOptions">FileRequestOptions customer input in TransferLocation.</param>
        /// <returns>FileRequestOptions instance with custom FileRequestOptions properties.</returns>
        public static FileRequestOptions GenerateFileRequestOptions(
            FileRequestOptions customRequestOptions)
        {
            if (null == customRequestOptions)
            {
                return Transfer_RequestOptions.DefaultFileRequestOptions;
            }
            else
            {
                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;

                AssignToRequestOptions(requestOptions, customRequestOptions);

                if (null != customRequestOptions.UseTransactionalMD5)
                {
                    requestOptions.UseTransactionalMD5 = customRequestOptions.UseTransactionalMD5;
                }

                requestOptions.DisableContentMD5Validation = customRequestOptions.DisableContentMD5Validation;
                return requestOptions;
            }
        }

        /// <summary>
        /// Generate an OperationContext from the the specified TransferContext.
        /// </summary>
        /// <param name="transferContext">Transfer context</param>
        /// <returns>An <see cref="OperationContext"/> object.</returns>
        public static OperationContext GenerateOperationContext(
            TransferContext transferContext)
        {
            if (transferContext == null)
            {
                return null;
            }
            
            return new OperationContext()
            {
                ClientRequestID = transferContext.ClientRequestId,
                LogLevel = transferContext.LogLevel,
            };
        }

        public static CloudBlob GetBlobReference(Uri blobUri, StorageCredentials credentials, BlobType blobType)
        {
            switch (blobType)
            {
                case BlobType.BlockBlob:
                    return new CloudBlockBlob(blobUri, credentials);
                case BlobType.PageBlob:
                    return new CloudPageBlob(blobUri, credentials);
                case BlobType.AppendBlob:
                    return new CloudAppendBlob(blobUri, credentials);
                default:
                    throw new InvalidOperationException(
                    string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.NotSupportedBlobType,
                    blobType));
            }
        }

        public static byte[] RequireBuffer(MemoryManager memoryManager, Action checkCancellation)
        {
            byte[] buffer;
            buffer = memoryManager.RequireBuffer();

            if (null == buffer)
            {
                int retryCount = 0;
                int retryInterval = 100;
                while ((retryCount < RequireBufferMaxRetryCount)
                    && (null == buffer))
                {
                    checkCancellation();
                    retryInterval <<= 1;
                    Thread.Sleep(retryInterval);
                    buffer = memoryManager.RequireBuffer();
                    ++retryCount;
                }
            }

            if (null == buffer)
            {
                throw new TransferException(
                    TransferErrorCode.FailToAllocateMemory,
                    Resources.FailedToAllocateMemoryException);
            }

            return buffer;
        }

        private static void AssignToRequestOptions(IRequestOptions targetRequestOptions, IRequestOptions customRequestOptions)
        {
            if (null != customRequestOptions.MaximumExecutionTime)
            {
                targetRequestOptions.MaximumExecutionTime = customRequestOptions.MaximumExecutionTime;
            }

            if (null != customRequestOptions.RetryPolicy)
            {
                targetRequestOptions.RetryPolicy = customRequestOptions.RetryPolicy;
            }

            if (null != customRequestOptions.ServerTimeout)
            {
                targetRequestOptions.ServerTimeout = customRequestOptions.ServerTimeout;
            }

            targetRequestOptions.LocationMode = customRequestOptions.LocationMode;
        }
    }
}
