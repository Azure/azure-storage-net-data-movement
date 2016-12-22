//------------------------------------------------------------------------------
// <copyright file="Utils.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;    
    using System.Net;
    using System.Text;
    using System.Threading;
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

        public static void CheckCancellation(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(Resources.TransferCancelledException);
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

        public static bool CompareProperties(Attributes first, Attributes second)
        {
            return string.Equals(first.CacheControl, second.CacheControl)
                && string.Equals(first.ContentDisposition, second.ContentDisposition)
                && string.Equals(first.ContentEncoding, second.ContentEncoding)
                && string.Equals(first.ContentLanguage, second.ContentLanguage)
                && string.Equals(first.ContentMD5, second.ContentMD5)
                && string.Equals(first.ContentType, second.ContentType);
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
        /// <param name="isCreationRequest">Indicate whether to generate request options for a CREATE requestion which requires shorter server timeout. </param>
        /// <returns>BlobRequestOptions instance with custom BlobRequestOptions properties.</returns>
        public static BlobRequestOptions GenerateBlobRequestOptions(
            BlobRequestOptions customRequestOptions, bool isCreationRequest = false)
        {

            var requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;

            if (isCreationRequest)
            {
                requestOptions.ServerTimeout = Transfer_RequestOptions.DefaultCreationServerTimeout;
            }
            
            if (null != customRequestOptions)
            {
                AssignToRequestOptions(requestOptions, customRequestOptions);

                if (null != customRequestOptions.UseTransactionalMD5)
                {
                    requestOptions.UseTransactionalMD5 = customRequestOptions.UseTransactionalMD5;
                }

                requestOptions.DisableContentMD5Validation = customRequestOptions.DisableContentMD5Validation;
            }

            return requestOptions;
        }

        /// <summary>
        /// Generate a FileRequestOptions with custom FileRequestOptions.
        /// We have default MaximumExecutionTime, ServerTimeout and RetryPolicy. 
        /// If user doesn't set these properties, we should use the default ones.
        /// Others, we should the custom ones.
        /// </summary>
        /// <param name="customRequestOptions">FileRequestOptions customer input in TransferLocation.</param>
        /// <param name="isCreationRequest">Indicate whether to generate request options for a CREATE requestion which requires shorter server timeout. </param>
        /// <returns>FileRequestOptions instance with custom FileRequestOptions properties.</returns>
        public static FileRequestOptions GenerateFileRequestOptions(
            FileRequestOptions customRequestOptions, bool isCreationRequest = false)
        {
            var requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;

            if (isCreationRequest)
            {
                requestOptions.ServerTimeout = Transfer_RequestOptions.DefaultCreationServerTimeout;
            }

            if (null != customRequestOptions)
            {
                AssignToRequestOptions(requestOptions, customRequestOptions);

                if (null != customRequestOptions.UseTransactionalMD5)
                {
                    requestOptions.UseTransactionalMD5 = customRequestOptions.UseTransactionalMD5;
                }

                requestOptions.DisableContentMD5Validation = customRequestOptions.DisableContentMD5Validation;
            }

            return requestOptions;
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

            OperationContext operationContext = new OperationContext()
            {
                LogLevel = transferContext.LogLevel
            };

            if (transferContext.ClientRequestId != null)
            {
                operationContext.ClientRequestID = transferContext.ClientRequestId;
            }

            return operationContext;
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
        public static string GetExceptionMessage(this Exception ex)
        {
            if (ex == null)
            {
                throw new ArgumentNullException("ex");
            }

            string exceptionMessage;
#if DEBUG
            exceptionMessage = ex.ToString();
#else
            var storageEx = ex as StorageException;
            if (storageEx != null)
            {
                exceptionMessage = storageEx.ToErrorDetail();
            }
            else
            {
                exceptionMessage = ex.Message;
            }
#endif

            return exceptionMessage;
        }
        
        /// <summary>
        /// Returns a string that represents error details of the corresponding <see cref="StorageException"/>.
        /// </summary>
        /// <param name="ex">The given exception.</param>
        /// <returns>A string that represents error details of the corresponding <see cref="StorageException"/>.</returns>
        public static string ToErrorDetail(this StorageException ex)
        {
            if (ex == null)
            {
                throw new ArgumentNullException("ex");
            }

            var messageBuilder = new StringBuilder();
            messageBuilder.Append(ex.Message);

            if (ex.RequestInformation != null)
            {
                string errorDetails = ex.RequestInformation.HttpStatusMessage;

                if (ex.RequestInformation.ExtendedErrorInformation != null)
                {
                    // Overrides the error details with error message inside
                    // extended error information if avaliable.
                    errorDetails = ex.RequestInformation.ExtendedErrorInformation.ErrorMessage;
                }
                else
                {
                    // If available, try to fetch the TimeoutException from the inner exception
                    // to provide more detail.
                    if (ex.InnerException != null)
                    {
                        var timeoutException = ex.InnerException as TimeoutException;
                        if (timeoutException != null)
                        {
                            errorDetails = timeoutException.Message;
                        }
                    }
                }

                messageBuilder.AppendLine();
                messageBuilder.Append(errorDetails);
            }

            return messageBuilder.ToString();
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

        public static bool IsExpectedHttpStatusCodes(StorageException e, params HttpStatusCode[] expectedStatusCodes)
        {
            if (e == null || e.RequestInformation == null)
            {
                return false;
            }

            int statusCode = e.RequestInformation.HttpStatusCode;
            foreach(HttpStatusCode expectedStatusCode in expectedStatusCodes)
            {
                if (statusCode == (int)expectedStatusCode)
                {
                    return true;
                }
            }

            return false;
        }
        
        /// <summary>
        /// Append snapshot time to a file name.
        /// </summary>
        /// <param name="fileName">Original file name.</param>
        /// <param name="snapshotTime">Snapshot time to append.</param>
        /// <returns>A file name with appended snapshot time.</returns>
        public static string AppendSnapShotTimeToFileName(string fileName, DateTimeOffset? snapshotTime)
        {
            string resultName = fileName;

            if (snapshotTime.HasValue)
            {
                string pathAndFileNameNoExt, extension;
                GetBasePathAndExtension(fileName, out pathAndFileNameNoExt, out extension);

                string timeStamp = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0:yyyy-MM-dd HHmmss fff}",
                    snapshotTime.Value);

                resultName = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} ({1}){2}",
                    pathAndFileNameNoExt,
                    timeStamp,
                    extension);
            }

            return resultName;
        }

        private static void GetBasePathAndExtension(string filePath, out string basePath, out string extension)
        {
            int index = filePath.LastIndexOf(".");
            extension = string.Empty;

            if (-1 == index)
            {
                basePath = filePath;
            }
            else
            {
                basePath = filePath.Substring(0, index);

                if (index < filePath.Length - 1)
                {
                    extension = filePath.Substring(index);
                }
            }
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
