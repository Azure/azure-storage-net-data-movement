//------------------------------------------------------------------------------
// <copyright file="Utils.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Class for various utils.
    /// </summary>
    internal static class Utils
    {
        private const int RequireBufferMaxRetryCount = 10;

        /// <summary>
        /// These filenames are reserved on windows, regardless of the file extension.
        /// </summary>
        private static readonly string[] ReservedBaseFileNames = new string[]
            {
                "CON", "PRN", "AUX", "NUL",
                "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
                "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
            };

        /// <summary>
        /// These filenames are reserved on windows, only if the full filename matches.
        /// </summary>
        private static readonly string[] ReservedFileNames = new string[]
            {
                "CLOCK$",
            };

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

        /// <summary>
        /// Check cancellation and throw <exception cref="OperationCanceledException"/>, if cancellation is requested.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public static void CheckCancellation(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(Resources.TransferCancelledException);
            }
        }

        public static bool CompareProperties(Attributes first, Attributes second)
        {
            if (first.CreationTime.HasValue)
            {
                if (!second.CreationTime.HasValue)
                    return false;

                if (first.CreationTime.Value != second.CreationTime.Value)
                    return false;
            }
            else if (second.CreationTime.HasValue)
            {
                return false;
            }

            if (first.LastWriteTime.HasValue)
            {
                if (!second.LastWriteTime.HasValue)
                    return false;

                if (first.LastWriteTime.Value != second.LastWriteTime.Value)
                    return false;
            }
            else if (second.LastWriteTime.HasValue)
            {
                return false;
            }

            if (first.CloudFileNtfsAttributes.HasValue)
            {
                if (!second.CloudFileNtfsAttributes.HasValue)
                    return false;

                if (first.CloudFileNtfsAttributes.Value != second.CloudFileNtfsAttributes.Value)
                    return false;
            }
            else if (second.CloudFileNtfsAttributes.HasValue)
            {
                return false;
            }

            return string.Equals(first.CacheControl, second.CacheControl)
                   && string.Equals(first.ContentDisposition, second.ContentDisposition)
                   && string.Equals(first.ContentEncoding, second.ContentEncoding)
                   && string.Equals(first.ContentLanguage, second.ContentLanguage)
                   && string.Equals(first.ContentMD5, second.ContentMD5)
                   && string.Equals(first.ContentType, second.ContentType);
        }

        public static bool DictionaryEquals(
            this IDictionary<string, string> firstDic, 
            IDictionary<string, string> secondDic)
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

        /// <summary>
        /// This is the wrapper for executing APIs defined in Azure storage .Net client library, 
        /// when wishing to return from thread pool immediately.
        /// As XSCL based on .Net Framework uses self-customized APM, and since XSCL 9.0.0.0, 
        /// .Net Core implementation changed async pattern, which removed Task.Run wrapper previously existed.
        /// This method uses Task.Run as calling wrapper for XSCL APIs, in order to return from thread pool immediately.
        /// </summary>
        /// <param name="func">Function to be called.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        /// <returns>The <see cref="Task"/>.</returns>
        public static async Task ExecuteXsclApiCallAsync(Func<Task> func, CancellationToken cancellationToken)
        {
            await Task.Run(async () => await func(), cancellationToken);
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

        public static Attributes GenerateAttributes(CloudFile file, bool preserveSMBProperties)
        {
            Attributes attributes = new Attributes()
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

            if (preserveSMBProperties)
            {
                attributes.CloudFileNtfsAttributes = file.Properties.NtfsAttributes;
                attributes.CreationTime = file.Properties.CreationTime;
                attributes.LastWriteTime = file.Properties.LastWriteTime;
            }

            return attributes;
        }

        /// <summary>
        /// Generate a BlobRequestOptions with custom BlobRequestOptions.
        /// We have default MaximumExecutionTime, ServerTimeout and RetryPolicy. 
        /// If user doesn't set these properties, we should use the default ones.
        /// Others, we should use the custom ones.
        /// </summary>
        /// <param name="customRequestOptions">BlobRequestOptions customer input in TransferLocation.</param>
        /// <param name="isCreationRequest">Indicate whether to generate request options for a CREATE requestion which requires shorter server timeout. </param>
        /// <returns>BlobRequestOptions instance with custom BlobRequestOptions properties.</returns>
        public static BlobRequestOptions GenerateBlobRequestOptions(
            BlobRequestOptions customRequestOptions, 
            bool isCreationRequest = false)
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

            if ((null != customCondition) && !string.IsNullOrEmpty(customCondition.LeaseId))
            {
                return AccessCondition.GenerateLeaseCondition(customCondition.LeaseId);
            }

            return null;
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
            FileRequestOptions customRequestOptions, 
            bool isCreationRequest = false)
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

        /// <summary>
        /// Generate an OperationContext from the the specified TransferContext.
        /// </summary>
        /// <param name="transferContext">Transfer context</param>
        /// <returns>An <see cref="OperationContext"/> object.</returns>
        public static OperationContext GenerateOperationContext(TransferContext transferContext)
        {
            OperationContext operationContext = new OperationContext()
                                                    {
                                                        CustomUserAgent =
                                                            string.Format(
                                                                CultureInfo.InvariantCulture, 
                                                                "{0} {1}", 
                                                                TransferManager.Configurations
                                                            .UserAgentPrefix, 
                                                                Constants.UserAgent)
                                                    };

            if (transferContext == null)
            {
                return operationContext;
            }

            operationContext.LogLevel = transferContext.LogLevel;

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
                        string.Format(CultureInfo.CurrentCulture, Resources.NotSupportedBlobType, blobType));
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

        public static bool IsExpectedHttpStatusCodes(StorageException e, params HttpStatusCode[] expectedStatusCodes)
        {
            if (e == null || e.RequestInformation == null)
            {
                return false;
            }

            int statusCode = e.RequestInformation.HttpStatusCode;
            foreach (HttpStatusCode expectedStatusCode in expectedStatusCodes)
            {
                if (statusCode == (int)expectedStatusCode)
                {
                    return true;
                }
            }

            return false;
        }

        public static byte[] RequireBuffer(MemoryManager memoryManager, Action checkCancellation)
        {
            byte[] buffer;
            buffer = memoryManager.RequireBuffer();

            if (null == buffer)
            {
                int retryCount = 0;
                int retryInterval = 100;
                while ((retryCount < RequireBufferMaxRetryCount) && (null == buffer))
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

        public static void SetAttributes(CloudFile file, Attributes attributes, bool preserveSMBProperties)
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

            if (preserveSMBProperties)
            {
                if (attributes.CloudFileNtfsAttributes.HasValue)
                {
                    file.Properties.NtfsAttributes = attributes.CloudFileNtfsAttributes.Value;
                }

                file.Properties.CreationTime = attributes.CreationTime;
                file.Properties.LastWriteTime = attributes.LastWriteTime;
            }
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

        public static IEnumerable<T> CatchException<T>(Func<IEnumerable<T>> srcEnumerable, Action<Exception> exceptionHandler)
        {
            IEnumerator<T> enumerator = null;
            bool next = true;
            try
            {
                enumerator = srcEnumerable().GetEnumerator();
                next = enumerator.MoveNext();
            }
            catch (Exception ex)
            {
                exceptionHandler(ex);
                yield break;
            }

            while (next)
            {
                yield return enumerator.Current;

                try
                {
                    next = enumerator.MoveNext();
                }
                catch (Exception ex)
                {
                    exceptionHandler(ex);
                    yield break;
                }
            }
        }

        public static void CreateCloudFileDirectoryRecursively(CloudFileDirectory dir)
        {
            if (null == dir)
            {
                return;
            }

            CloudFileDirectory parent = dir.Parent;

            // null == parent means dir is root directory, 
            // we should not call CreateIfNotExists in that case
            if (null != parent)
            {
                CreateCloudFileDirectoryRecursively(parent);

                try
                {
                    // create anyway, ignore 409 and 403
                    dir.CreateAsync(Transfer_RequestOptions.DefaultFileRequestOptions, null).GetAwaiter().GetResult();
                }
                catch (StorageException se)
                {
                    if (!IgnoreDirectoryCreationError(se))
                    {
                        throw;
                    }
                }
            }
        }

        public static void ValidateDestinationPath(string sourcePath, string destPath)
        {
            if (Interop.CrossPlatformHelpers.IsWindows)
            {
                if (!IsValidWindowsFileName(destPath))
                {
                    throw new TransferException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.SourceNameInvalidInFileSystem,
                            sourcePath));
                }
            }
        }

        public static void CreateParentDirectoryIfNotExists(string path)
        {
            string dir = LongPath.GetDirectoryName(path);

            if (!string.IsNullOrEmpty(dir) && !LongPathDirectory.Exists(dir))
            {
                LongPathDirectory.CreateDirectory(dir);
            }
        }

#if DEBUG
        public static void HandleFaultInjection(string relativePath, SingleObjectTransfer transfer)
        {
            FaultInjectionPoint fip = new FaultInjectionPoint(FaultInjectionPoint.FIP_ThrowExceptionAfterEnumerated);
            string fiValue;
            string filePath = relativePath;
            CloudBlob sourceBlob = transfer.Source.Instance as CloudBlob;
            if (sourceBlob != null && sourceBlob.IsSnapshot)
            {
                filePath = Utils.AppendSnapShotTimeToFileName(filePath, sourceBlob.SnapshotTime);
            }

            if (fip.TryGetValue(out fiValue)
                && string.Equals(fiValue, filePath, StringComparison.OrdinalIgnoreCase))
            {
                throw new TransferException(TransferErrorCode.Unknown, "test exception thrown because of ThrowExceptionAfterEnumerated is enabled", null);
            }
        }
#endif
        public static FileAttributes AzureFileNtfsAttributesToLocalAttributes(CloudFileNtfsAttributes cloudFileNtfsAttributes)
        {
            FileAttributes fileAttributes = FileAttributes.Normal;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.ReadOnly) == CloudFileNtfsAttributes.ReadOnly)
                fileAttributes |= FileAttributes.ReadOnly;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Hidden) == CloudFileNtfsAttributes.Hidden)
                fileAttributes |= FileAttributes.Hidden;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.System) == CloudFileNtfsAttributes.System)
                fileAttributes |= FileAttributes.System;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Directory) == CloudFileNtfsAttributes.Directory)
                fileAttributes |= FileAttributes.Directory;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Archive) == CloudFileNtfsAttributes.Archive)
                fileAttributes |= FileAttributes.Archive;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Temporary) == CloudFileNtfsAttributes.Temporary)
                fileAttributes |= FileAttributes.Temporary;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Offline) == CloudFileNtfsAttributes.Offline)
                fileAttributes |= FileAttributes.Offline;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.NotContentIndexed) == CloudFileNtfsAttributes.NotContentIndexed)
                fileAttributes |= FileAttributes.NotContentIndexed;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.NoScrubData) == CloudFileNtfsAttributes.NoScrubData)
                fileAttributes |= FileAttributes.NoScrubData;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Normal) == CloudFileNtfsAttributes.None)
            {
                if (fileAttributes != FileAttributes.Normal)
                {
                    fileAttributes = fileAttributes & (~FileAttributes.Normal);
                }
            }

            return fileAttributes;
        }

        public static CloudFileNtfsAttributes LocalAttributesToAzureFileNtfsAttributes(FileAttributes fileAttributes)
        {
            CloudFileNtfsAttributes cloudFileNtfsAttributes = CloudFileNtfsAttributes.None;

            if ((fileAttributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.ReadOnly;

            if ((fileAttributes & FileAttributes.Hidden) == FileAttributes.Hidden)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Hidden;

            if ((fileAttributes & FileAttributes.System) == FileAttributes.System)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.System;

            if ((fileAttributes & FileAttributes.Directory) == FileAttributes.Directory)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Directory;

            if ((fileAttributes & FileAttributes.Archive) == FileAttributes.Archive)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Archive;

            if ((fileAttributes & FileAttributes.Normal) == FileAttributes.Normal)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Normal;

            if ((fileAttributes & FileAttributes.Temporary) == FileAttributes.Temporary)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Temporary;

            if ((fileAttributes & FileAttributes.Offline) == FileAttributes.Offline)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Offline;

            if ((fileAttributes & FileAttributes.NotContentIndexed) == FileAttributes.NotContentIndexed)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.NotContentIndexed;

            if ((fileAttributes & FileAttributes.NoScrubData) == FileAttributes.NoScrubData)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.NoScrubData;

            if (cloudFileNtfsAttributes == CloudFileNtfsAttributes.None) cloudFileNtfsAttributes = CloudFileNtfsAttributes.Normal;

            return cloudFileNtfsAttributes;
        }

        private static bool IsValidWindowsFileName(string fileName)
        {
            string fileNameNoExt = LongPath.GetFileNameWithoutExtension(fileName);
            string fileNameWithExt = LongPath.GetFileName(fileName);

            if (Array.Exists<string>(ReservedBaseFileNames, delegate (string s) { return fileNameNoExt.Equals(s, StringComparison.OrdinalIgnoreCase); }))
            {
                return false;
            }

            if (Array.Exists<string>(ReservedFileNames, delegate (string s) { return fileNameWithExt.Equals(s, StringComparison.OrdinalIgnoreCase); }))
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(fileNameWithExt))
            {
                return false;
            }

            bool allDotsOrWhiteSpace = true;
            for (int i = 0; i < fileName.Length; ++i)
            {
                if (fileName[i] != '.' && !char.IsWhiteSpace(fileName[i]))
                {
                    allDotsOrWhiteSpace = false;
                    break;
                }
            }

            if (allDotsOrWhiteSpace)
            {
                return false;
            }

            return true;
        }

        private static bool IgnoreDirectoryCreationError(StorageException se)
        {
            if (null == se)
            {
                return false;
            }

            if (Utils.IsExpectedHttpStatusCodes(se, HttpStatusCode.Forbidden))
            {
                return true;
            }

            if (null != se.RequestInformation
                && se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict
                && string.Equals(se.RequestInformation.ErrorCode, "ResourceAlreadyExists"))
            {
                return true;
            }

            return false;
        }

        private static void AssignToRequestOptions(
            IRequestOptions targetRequestOptions, 
            IRequestOptions customRequestOptions)
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

        private static void GetBasePathAndExtension(string filePath, out string basePath, out string extension)
        {
            basePath = filePath;
            extension = string.Empty;

            if (filePath != null)
            {
                for (int i = filePath.Length; --i >= 0;)
                {
                    char ch = filePath[i];
                    if (ch == '.')
                    {
                        basePath = filePath.Substring(0, i);

                        if (i != filePath.Length - 1)
                        {
                            extension = filePath.Substring(i, filePath.Length - i);
                        }

                        break;
                    }

                    if (ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar
                        || ch == Path.VolumeSeparatorChar)
                    {
                        break;
                    }
                }
            }
        }
    }
}