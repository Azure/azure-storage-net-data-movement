//-----------------------------------------------------------------------------
// <copyright file="TransferManager.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.WindowsAzure.Storage.File;
    using TransferKey = System.Tuple<TransferLocation, TransferLocation>;

    /// <summary>
    /// TransferManager class
    /// </summary>
    public static class TransferManager
    {
        /// <summary>
        /// Transfer scheduler that schedules execution of transfer jobs
        /// </summary>
        private static TransferScheduler scheduler = new TransferScheduler();

        /// <summary>
        /// Transfer configurations associated with the transfer manager
        /// </summary>
        private static TransferConfigurations configurations = new TransferConfigurations();

        /// <summary>
        /// Stores all running transfers
        /// </summary>
        private static ConcurrentDictionary<TransferKey, Transfer> allTransfers = new ConcurrentDictionary<TransferKey, Transfer>();

#if REQUEST_EVENT_ARGS_EXPOSES_REQUEST // DNX profiles don't expose the HttpWebRequest through RequestEventArgs, so the user agent cannot be set
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", Justification = "Performance")]
        static TransferManager()
        {
            OperationContext.GlobalSendingRequest += (sender, args) =>
            {
                // User agent string format: [UserAgentPrefix] <DMLib product token> <XSCL product token>
                // e.g. Suppose UserAgentPrefix is "MyApp", the overall user agent string would be like
                // 'MyApp DataMovement/0.5.0.0 WA-Storage/8.0.0 (.NET CLR 4.0.30319.34014; Win32NT 6.2.9200.0)' 
                string userAgent = Constants.UserAgent + " " + Microsoft.WindowsAzure.Storage.Shared.Protocol.Constants.HeaderConstants.UserAgent;

                if (!string.IsNullOrEmpty(configurations.UserAgentPrefix))
                {
                    userAgent = configurations.UserAgentPrefix + " " + userAgent;
                }
                
                args.Request.UserAgent = userAgent;
            };
        }
#endif // REQUEST_EVENT_ARGS_EXPOSES_REQUEST

        /// <summary>
        /// Gets or sets the transfer configurations associated with the transfer manager
        /// </summary>
        public static TransferConfigurations Configurations
        {
            get
            {
                return configurations;
            }
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudBlob destBlob)
        {
            return UploadAsync(sourcePath, destBlob, null, null);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudBlob destBlob, UploadOptions options, SingleTransferContext context)
        {
            return UploadAsync(sourcePath, destBlob, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task UploadAsync(string sourcePath, CloudBlob destBlob, UploadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            FileLocation sourceLocation = new FileLocation(sourcePath);
            AzureBlobLocation destLocation = new AzureBlobLocation(destBlob);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return UploadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudBlob destBlob)
        {
            return UploadAsync(sourceStream, destBlob, null, null);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudBlob destBlob, UploadOptions options, SingleTransferContext context)
        {
            return UploadAsync(sourceStream, destBlob, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="Stream"/> object providing the file content.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task UploadAsync(Stream sourceStream, CloudBlob destBlob, UploadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            StreamLocation sourceLocation = new StreamLocation(sourceStream);
            AzureBlobLocation destLocation = new AzureBlobLocation(destBlob);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return UploadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudFile destFile)
        {
            return UploadAsync(sourcePath, destFile, null, null);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudFile destFile, UploadOptions options, SingleTransferContext context)
        {
            return UploadAsync(sourcePath, destFile, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task UploadAsync(string sourcePath, CloudFile destFile, UploadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            FileLocation sourceLocation = new FileLocation(sourcePath);
            AzureFileLocation destLocation = new AzureFileLocation(destFile);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return UploadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudFile destFile)
        {
            return UploadAsync(sourceStream, destFile, null, null);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudFile destFile, UploadOptions options, SingleTransferContext context)
        {
            return UploadAsync(sourceStream, destFile, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="Stream"/> object providing the file content.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task UploadAsync(Stream sourceStream, CloudFile destFile, UploadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            StreamLocation sourceLocation = new StreamLocation(sourceStream);
            AzureFileLocation destLocation = new AzureFileLocation(destFile);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return UploadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, string destPath)
        {
            return DownloadAsync(sourceBlob, destPath, null, null);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, string destPath, DownloadOptions options, SingleTransferContext context)
        {
            return DownloadAsync(sourceBlob, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a directory to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudBlobDirectory destBlobDir)
        {
            return UploadDirectoryAsync(sourcePath, destBlobDir, null, null);
        }

        /// <summary>
        /// Upload a directory to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="options">An <see cref="UploadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudBlobDirectory destBlobDir, UploadDirectoryOptions options, DirectoryTransferContext context)
        {
            return UploadDirectoryAsync(sourcePath, destBlobDir, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a directory to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="options">An <see cref="UploadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudBlobDirectory destBlobDir, UploadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            DirectoryLocation sourceLocation = new DirectoryLocation(sourcePath);
            AzureBlobDirectoryLocation destLocation = new AzureBlobDirectoryLocation(destBlobDir);
            FileEnumerator sourceEnumerator = new FileEnumerator(sourceLocation, null != options? options.FollowSymlink : false);
            if (options != null)
            {
                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
            }

            return UploadDirectoryInternalAsync(sourceLocation, destLocation, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Upload a directory to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudFileDirectory destFileDir)
        {
            return UploadDirectoryAsync(sourcePath, destFileDir, null, null);
        }

        /// <summary>
        /// Upload a directory to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="options">An <see cref="UploadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudFileDirectory destFileDir, UploadDirectoryOptions options, DirectoryTransferContext context)
        {
            return UploadDirectoryAsync(sourcePath, destFileDir, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a directory to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source directory</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="options">An <see cref="UploadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> UploadDirectoryAsync(string sourcePath, CloudFileDirectory destFileDir, UploadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            DirectoryLocation sourceLocation = new DirectoryLocation(sourcePath);
            AzureFileDirectoryLocation destLocation = new AzureFileDirectoryLocation(destFileDir);
            FileEnumerator sourceEnumerator = new FileEnumerator(sourceLocation, null == options ? false : options.FollowSymlink);
            if (options != null)
            {
                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
            }

            return UploadDirectoryInternalAsync(sourceLocation, destLocation, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task DownloadAsync(CloudBlob sourceBlob, string destPath, DownloadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobLocation sourceLocation = new AzureBlobLocation(sourceBlob);
            FileLocation destLocation = new FileLocation(destPath);

            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;

                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                requestOptions.UseTransactionalMD5 = options.UseTransactionalMD5;
                sourceLocation.BlobRequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, Stream destStream)
        {
            return DownloadAsync(sourceBlob, destStream, null, null);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, Stream destStream, DownloadOptions options, SingleTransferContext context)
        {
            return DownloadAsync(sourceBlob, destStream, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destStream">A <see cref="Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task DownloadAsync(CloudBlob sourceBlob, Stream destStream, DownloadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobLocation sourceLocation = new AzureBlobLocation(sourceBlob);
            StreamLocation destLocation = new StreamLocation(destStream);

            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;

                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                requestOptions.UseTransactionalMD5 = options.UseTransactionalMD5;
                sourceLocation.BlobRequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, string destPath)
        {
            return DownloadAsync(sourceFile, destPath, null, null);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, string destPath, DownloadOptions options, SingleTransferContext context)
        {
            return DownloadAsync(sourceFile, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task DownloadAsync(CloudFile sourceFile, string destPath, DownloadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileLocation sourceLocation = new AzureFileLocation(sourceFile);
            FileLocation destLocation = new FileLocation(destPath);

            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;

                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.FileRequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, Stream destStream)
        {
            return DownloadAsync(sourceFile, destStream, null, null);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, Stream destStream, DownloadOptions options, SingleTransferContext context)
        {
            return DownloadAsync(sourceFile, destStream, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destStream">A <see cref="Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task DownloadAsync(CloudFile sourceFile, Stream destStream, DownloadOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileLocation sourceLocation = new AzureFileLocation(sourceFile);
            StreamLocation destLocation = new StreamLocation(destStream);

            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;

                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.FileRequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure blob directory from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destPath">Path to the destination directory</param>
        /// <param name="options">A <see cref="DownloadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> DownloadDirectoryAsync(CloudBlobDirectory sourceBlobDir, string destPath, DownloadDirectoryOptions options, DirectoryTransferContext context)
        {
            return DownloadDirectoryAsync(sourceBlobDir, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure blob directory from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destPath">Path to the destination directory</param>
        /// <param name="options">A <see cref="DownloadDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> DownloadDirectoryAsync(CloudBlobDirectory sourceBlobDir, string destPath, DownloadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobDirectoryLocation sourceLocation = new AzureBlobDirectoryLocation(sourceBlobDir);
            DirectoryLocation destLocation = new DirectoryLocation(destPath);
            AzureBlobEnumerator sourceEnumerator = new AzureBlobEnumerator(sourceLocation);
            if (options != null)
            {
                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
                sourceEnumerator.IncludeSnapshots = options.IncludeSnapshots;

                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.BlobRequestOptions = requestOptions;
            }

            return DownloadDirectoryInternalAsync(sourceLocation, destLocation, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Download an Azure file directory from Azure File Storage.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destPath">Path to the destination directory</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> DownloadDirectoryAsync(CloudFileDirectory sourceFileDir, string destPath, DownloadDirectoryOptions options, DirectoryTransferContext context)
        {
            return DownloadDirectoryAsync(sourceFileDir, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure file directory from Azure File Storage.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destPath">Path to the destination directory</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> DownloadDirectoryAsync(CloudFileDirectory sourceFileDir, string destPath, DownloadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileDirectoryLocation sourceLocation = new AzureFileDirectoryLocation(sourceFileDir);
            DirectoryLocation destLocation = new DirectoryLocation(destPath);
            AzureFileEnumerator sourceEnumerator = new AzureFileEnumerator(sourceLocation);
            if (options != null)
            {
                TransferManager.CheckSearchPatternOfAzureFileSource(options);

                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;

                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.FileRequestOptions = requestOptions;
            }

            return DownloadDirectoryInternalAsync(sourceLocation, destLocation, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Copy content, properties and metadata of one Azure blob to another.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudBlob destBlob, bool isServiceCopy)
        {
            return CopyAsync(sourceBlob, destBlob, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy content, properties and metadata of one Azure blob to another.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceBlob, destBlob, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy content, properties and metadata of one Azure blob to another.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(CloudBlob sourceBlob, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobLocation sourceLocation = new AzureBlobLocation(sourceBlob);
            AzureBlobLocation destLocation = new AzureBlobLocation(destBlob);
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure blob to an Azure file.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudFile destFile, bool isServiceCopy)
        {
            return CopyAsync(sourceBlob, destFile, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure blob to an Azure file.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceBlob, destFile, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure blob to an Azure file.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(CloudBlob sourceBlob, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobLocation sourceLocation = new AzureBlobLocation(sourceBlob);
            AzureFileLocation destLocation = new AzureFileLocation(destFile);
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure file to an Azure blob.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudBlob destBlob, bool isServiceCopy)
        {
            return CopyAsync(sourceFile, destBlob, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure file to an Azure blob.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceFile, destBlob, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure file to an Azure blob.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(CloudFile sourceFile, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileLocation sourceLocation = new AzureFileLocation(sourceFile);
            AzureBlobLocation destLocation = new AzureBlobLocation(destBlob);
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }


        /// <summary>
        /// Copy content, properties and metadata of an Azure file to another.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudFile destFile, bool isServiceCopy)
        {
            return CopyAsync(sourceFile, destFile, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure file to another.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceFile, destFile, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy content, properties and metadata of an Azure file to another.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(CloudFile sourceFile, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileLocation sourceLocation = new AzureFileLocation(sourceFile);
            AzureFileLocation destLocation = new AzureFileLocation(destFile);
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure blob.
        /// </summary>
        /// <param name="sourceUri">The <see cref="System.Uri"/> of the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure blob synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudBlob destBlob, bool isServiceCopy)
        {
            return CopyAsync(sourceUri, destBlob, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure blob.
        /// </summary>
        /// <param name="sourceUri">The <see cref="System.Uri"/> of the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure blob synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceUri, destBlob, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure blob.
        /// </summary>
        /// <param name="sourceUri">The <see cref="Uri"/> of the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure blob synchronously is not supported yet.</remarks>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(Uri sourceUri, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            if (!isServiceCopy)
            {
                throw new NotSupportedException(Resources.SyncCopyFromUriToAzureBlobNotSupportedException);
            }

            UriLocation sourceLocation = new UriLocation(sourceUri);
            AzureBlobLocation destLocation = new AzureBlobLocation(destBlob);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure file.
        /// </summary>
        /// <param name="sourceUri">The <see cref="System.Uri"/> of the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure file synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudFile destFile, bool isServiceCopy)
        {
            return CopyAsync(sourceUri, destFile, isServiceCopy, null, null);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure file.
        /// </summary>
        /// <param name="sourceUri">The <see cref="System.Uri"/> of the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure file synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context)
        {
            return CopyAsync(sourceUri, destFile, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy file from an specified URI to an Azure file.
        /// </summary>
        /// <param name="sourceUri">The <see cref="Uri"/> of the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="SingleTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure file synchronously is not supported yet.</remarks>
        [SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters", Justification = "With TransferContext, it also accepts DirectoryTransferContext. Here forbid this behavior.")]
        public static Task CopyAsync(Uri sourceUri, CloudFile destFile, bool isServiceCopy, CopyOptions options, SingleTransferContext context, CancellationToken cancellationToken)
        {
            if (!isServiceCopy)
            {
                throw new NotSupportedException(Resources.SyncCopyFromUriToAzureFileNotSupportedException);
            }

            UriLocation sourceLocation = new UriLocation(sourceUri);
            AzureFileLocation destLocation = new AzureFileLocation(destFile);
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, context, cancellationToken);
        }

        /// <summary>
        /// Copy an Azure blob directory to another Azure blob directory.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudBlobDirectory sourceBlobDir, CloudBlobDirectory destBlobDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context)
        {
            return CopyDirectoryAsync(sourceBlobDir, destBlobDir, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy an Azure blob directory to another Azure blob directory.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudBlobDirectory sourceBlobDir, CloudBlobDirectory destBlobDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobDirectoryLocation sourceLocation = new AzureBlobDirectoryLocation(sourceBlobDir);
            AzureBlobDirectoryLocation destLocation = new AzureBlobDirectoryLocation(destBlobDir);
            AzureBlobEnumerator sourceEnumerator = new AzureBlobEnumerator(sourceLocation);
            if (options != null)
            {
                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
                sourceEnumerator.IncludeSnapshots = options.IncludeSnapshots;
            }

            return CopyDirectoryInternalAsync(sourceLocation, destLocation, isServiceCopy, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Copy an Azure blob directory to an Azure file directory.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudBlobDirectory sourceBlobDir, CloudFileDirectory destFileDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context)
        {
            return CopyDirectoryAsync(sourceBlobDir, destFileDir, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy an Azure blob directory to an Azure file directory.
        /// </summary>
        /// <param name="sourceBlobDir">The <see cref="CloudBlobDirectory"/> that is the source Azure blob directory.</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudBlobDirectory sourceBlobDir, CloudFileDirectory destFileDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureBlobDirectoryLocation sourceLocation = new AzureBlobDirectoryLocation(sourceBlobDir);
            AzureFileDirectoryLocation destLocation = new AzureFileDirectoryLocation(destFileDir);
            AzureBlobEnumerator sourceEnumerator = new AzureBlobEnumerator(sourceLocation);
            if (options != null)
            {
                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
                sourceEnumerator.IncludeSnapshots = options.IncludeSnapshots;
            }

            return CopyDirectoryInternalAsync(sourceLocation, destLocation, isServiceCopy, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Copy an Azure file directory to another Azure file directory.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudFileDirectory sourceFileDir, CloudFileDirectory destFileDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context)
        {
            return CopyDirectoryAsync(sourceFileDir, destFileDir, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy an Azure file directory to another Azure file directory.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destFileDir">The <see cref="CloudFileDirectory"/> that is the destination Azure file directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudFileDirectory sourceFileDir, CloudFileDirectory destFileDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileDirectoryLocation sourceLocation = new AzureFileDirectoryLocation(sourceFileDir);
            AzureFileDirectoryLocation destLocation = new AzureFileDirectoryLocation(destFileDir);
            AzureFileEnumerator sourceEnumerator = new AzureFileEnumerator(sourceLocation);
            if (options != null)
            {
                TransferManager.CheckSearchPatternOfAzureFileSource(options);

                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
            }

            return CopyDirectoryInternalAsync(sourceLocation, destLocation, isServiceCopy, sourceEnumerator, options, context, cancellationToken);
        }

        /// <summary>
        /// Copy an Azure file directory to an Azure blob directory.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudFileDirectory sourceFileDir, CloudBlobDirectory destBlobDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context)
        {
            return CopyDirectoryAsync(sourceFileDir, destBlobDir, isServiceCopy, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Copy an Azure file directory to an Azure blob directory.
        /// </summary>
        /// <param name="sourceFileDir">The <see cref="CloudFileDirectory"/> that is the source Azure file directory.</param>
        /// <param name="destBlobDir">The <see cref="CloudBlobDirectory"/> that is the destination Azure blob directory.</param>
        /// <param name="isServiceCopy">A flag indicating whether the copy is service-side asynchronous copy or not.
        /// If this flag is set to true, service-side asychronous copy will be used; if this flag is set to false,
        /// file is downloaded from source first, then uploaded to destination.</param>
        /// <param name="options">A <see cref="CopyDirectoryOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="DirectoryTransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="TransferStatus"/> that represents the asynchronous operation.</returns>
        public static Task<TransferStatus> CopyDirectoryAsync(CloudFileDirectory sourceFileDir, CloudBlobDirectory destBlobDir, bool isServiceCopy, CopyDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            AzureFileDirectoryLocation sourceLocation = new AzureFileDirectoryLocation(sourceFileDir);
            AzureBlobDirectoryLocation destLocation = new AzureBlobDirectoryLocation(destBlobDir);
            AzureFileEnumerator sourceEnumerator = new AzureFileEnumerator(sourceLocation);
            if (options != null)
            {
                TransferManager.CheckSearchPatternOfAzureFileSource(options);

                sourceEnumerator.SearchPattern = options.SearchPattern;
                sourceEnumerator.Recursive = options.Recursive;
            }

            return CopyDirectoryInternalAsync(sourceLocation, destLocation, isServiceCopy, sourceEnumerator, options, context, cancellationToken);
        }

        internal static void SetMemoryLimitation(long memoryLimitation)
        {
            scheduler?.MemoryManager.SetMemoryLimitation(memoryLimitation);
        }

        private static Task UploadInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, TransferContext context, CancellationToken cancellationToken)
        {
            Transfer transfer = GetOrCreateSingleObjectTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);
            return DoTransfer(transfer, context, cancellationToken);
        }

        private static Task DownloadInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, TransferContext context, CancellationToken cancellationToken)
        {
            Transfer transfer = GetOrCreateSingleObjectTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);
            return DoTransfer(transfer, context, cancellationToken);
        }

        private static Task CopyInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, bool isServiceCopy, TransferContext context, CancellationToken cancellationToken)
        {
            Transfer transfer = GetOrCreateSingleObjectTransfer(sourceLocation, destLocation, isServiceCopy ? TransferMethod.AsyncCopy : TransferMethod.SyncCopy, context);
            return DoTransfer(transfer, context, cancellationToken);
        }

        private static async Task<TransferStatus> UploadDirectoryInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, ITransferEnumerator sourceEnumerator, UploadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            DirectoryTransfer transfer = GetOrCreateDirectoryTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);

            if (transfer.SourceEnumerator == null || !AreSameTransferEnumerators(transfer.SourceEnumerator, sourceEnumerator))
            {
                transfer.SourceEnumerator = sourceEnumerator;
            }

            if (options != null)
            {
                transfer.BlobType = options.BlobType;
            }

            await DoTransfer(transfer, context, cancellationToken);

            return TransferManager.CreateTransferSummary(transfer.ProgressTracker);
        }

        private static async Task<TransferStatus> DownloadDirectoryInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, ITransferEnumerator sourceEnumerator, DownloadDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            DirectoryTransfer transfer = GetOrCreateDirectoryTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);

            if (null != options)
            {
                transfer.Delimiter = options.Delimiter;
            }
            
            if (transfer.SourceEnumerator == null || !AreSameTransferEnumerators(transfer.SourceEnumerator, sourceEnumerator))
            {
                transfer.SourceEnumerator = sourceEnumerator;
            }

            await DoTransfer(transfer, context, cancellationToken);

            return TransferManager.CreateTransferSummary(transfer.ProgressTracker);
        }

        private static async Task<TransferStatus> CopyDirectoryInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, bool isServiceCopy, ITransferEnumerator sourceEnumerator, CopyDirectoryOptions options, DirectoryTransferContext context, CancellationToken cancellationToken)
        {
            DirectoryTransfer transfer = GetOrCreateDirectoryTransfer(sourceLocation, destLocation, isServiceCopy ? TransferMethod.AsyncCopy : TransferMethod.SyncCopy, context);
            
            if (transfer.SourceEnumerator == null || !AreSameTransferEnumerators(transfer.SourceEnumerator, sourceEnumerator))
            {
                transfer.SourceEnumerator = sourceEnumerator;
            }

            if (options != null)
            {
                transfer.BlobType = options.BlobType;
                transfer.Delimiter = options.Delimiter;
            }

            await DoTransfer(transfer, context, cancellationToken);

            return TransferManager.CreateTransferSummary(transfer.ProgressTracker);
        }

        private static async Task DoTransfer(Transfer transfer, TransferContext transferContext, CancellationToken cancellationToken)
        {
            using (transfer)
            {
                if (!TryAddTransfer(transfer))
                {
                    throw new TransferException(TransferErrorCode.TransferAlreadyExists, Resources.TransferAlreadyExists);
                }

                if (transferContext != null)
                {
                    if (transfer.Context == null)
                    {
                        // associate transfer with transfer context
                        transfer.Context = transferContext;
                    }
                }

                try
                {
                    await transfer.ExecuteAsync(scheduler, cancellationToken);
                }
                finally
                {
                    RemoveTransfer(transfer);
                }
            }
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object will be disposed by the caller.")]
        private static SingleObjectTransfer GetOrCreateSingleObjectTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod, TransferContext transferContext)
        {
            SingleObjectTransfer singleObjectTransfer = null;
            Transfer transfer = GetTransfer(sourceLocation, destLocation, transferMethod, transferContext);
            if (transfer == null)
            {
                singleObjectTransfer = new SingleObjectTransfer(sourceLocation, destLocation, transferMethod);

                if (transferContext != null)
                {
                    transferContext.Checkpoint.AddTransfer(singleObjectTransfer);
                }
            }
            else
            {
                singleObjectTransfer = transfer as SingleObjectTransfer;
                Debug.Assert(singleObjectTransfer != null, "singleObjectTransfer");
            }

            return singleObjectTransfer;
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object will be disposed by the caller.")]
        private static DirectoryTransfer GetOrCreateDirectoryTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod, TransferContext transferContext)
        {
            DirectoryTransfer directoryTransfer = null;
            Transfer transfer = GetTransfer(sourceLocation, destLocation, transferMethod, transferContext);
            if (transfer == null)
            {
                directoryTransfer = new DirectoryTransfer(sourceLocation, destLocation, transferMethod);

                if (transferContext != null)
                {
                    transferContext.Checkpoint.AddTransfer(directoryTransfer);
                }
            }
            else
            {
                directoryTransfer = transfer as DirectoryTransfer;
                Debug.Assert(directoryTransfer != null, "directoryTransfer");
            }

            directoryTransfer.MaxTransferConcurrency = configurations.ParallelOperations * Constants.ListSegmentLengthMultiplier;
            return directoryTransfer;
        }

        private static Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod, TransferContext transferContext)
        {
            Transfer transfer = null;
            if (transferContext != null)
            {
                transfer = transferContext.Checkpoint.GetTransfer(sourceLocation, destLocation, transferMethod);
                if (transfer != null)
                {
                    if (sourceLocation is StreamLocation || destLocation is StreamLocation)
                    {
                        throw new TransferException(Resources.ResumeStreamTransferNotSupported);
                    }

                    // update transfer location information
                    UpdateTransferLocation(transfer.Source, sourceLocation);
                    UpdateTransferLocation(transfer.Destination, destLocation);
                }
            }

            return transfer;
        }

        private static bool TryAddTransfer(Transfer transfer)
        {
            return allTransfers.TryAdd(new TransferKey(transfer.Source, transfer.Destination), transfer);
        }

        private static void RemoveTransfer(Transfer transfer)
        {
            Transfer unused = null;
            allTransfers.TryRemove(new TransferKey(transfer.Source, transfer.Destination), out unused);
        }

        private static void UpdateTransferLocation(TransferLocation targetLocation, TransferLocation location)
        {
            // update storage credentials
            if (targetLocation.Type == TransferLocationType.AzureBlob)
            {
                AzureBlobLocation blobLocation = location as AzureBlobLocation;
                (targetLocation as AzureBlobLocation).UpdateCredentials(blobLocation.Blob.ServiceClient.Credentials);
            }
            else if (targetLocation.Type == TransferLocationType.AzureFile)
            {
                AzureFileLocation fileLocation = location as AzureFileLocation;
                (targetLocation as AzureFileLocation).UpdateCredentials(fileLocation.AzureFile.ServiceClient.Credentials);
            }
            else if (targetLocation.Type == TransferLocationType.AzureBlobDirectory)
            {
                AzureBlobDirectoryLocation blobDirectoryLocation = location as AzureBlobDirectoryLocation;
                (targetLocation as AzureBlobDirectoryLocation).UpdateCredentials(blobDirectoryLocation.BlobDirectory.ServiceClient.Credentials);
            }
            else if (targetLocation.Type == TransferLocationType.AzureFileDirectory)
            {
                AzureFileDirectoryLocation fileDirectoryLocation = location as AzureFileDirectoryLocation;
                (targetLocation as AzureFileDirectoryLocation).UpdateCredentials(fileDirectoryLocation.FileDirectory.ServiceClient.Credentials);
            }
        }

        private static bool AreSameTransferEnumerators(ITransferEnumerator enumerator1, ITransferEnumerator enumerator2)
        {
            TransferEnumeratorBase enumeratorBase1 = enumerator1 as TransferEnumeratorBase;
            TransferEnumeratorBase enumeratorBase2 = enumerator2 as TransferEnumeratorBase;

            if (enumeratorBase1 != null && enumeratorBase2 != null)
            {
                if ((enumeratorBase1.Recursive == enumeratorBase2.Recursive) &&
                    string.Equals(enumeratorBase1.SearchPattern, enumeratorBase2.SearchPattern, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private static void CheckSearchPatternOfAzureFileSource(DirectoryOptions options)
        {
            if (options.Recursive && !string.IsNullOrEmpty(options.SearchPattern))
            {
                throw new NotSupportedException(Resources.SearchPatternInRecursiveModeFromAzureFileNotSupportedException);
            }
        }

        private static TransferStatus CreateTransferSummary(TransferProgressTracker progress)
        {
            return new TransferStatus()
            {
                BytesTransferred = progress.BytesTransferred,
                NumberOfFilesTransferred = progress.NumberOfFilesTransferred,
                NumberOfFilesSkipped = progress.NumberOfFilesSkipped,
                NumberOfFilesFailed = progress.NumberOfFilesFailed
            };
        }
    }
}
