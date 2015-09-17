//-----------------------------------------------------------------------------
// <copyright file="TransferManager.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", Justification = "Performance")]
        static TransferManager()
        {
            OperationContext.GlobalSendingRequest += (sender, args) =>
            {
                string userAgent = Constants.UserAgent + ";" + Microsoft.WindowsAzure.Storage.Shared.Protocol.Constants.HeaderConstants.UserAgent;

                if (!string.IsNullOrEmpty(configurations.UserAgentSuffix))
                {
                    userAgent += ";" + configurations.UserAgentSuffix;
                }

                args.Request.UserAgent = userAgent;
            };
        }

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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudBlob destBlob, UploadOptions options, TransferContext context)
        {
            return UploadAsync(sourcePath, destBlob, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        public static Task UploadAsync(string sourcePath, CloudBlob destBlob, UploadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourcePath);
            TransferLocation destLocation = new TransferLocation(destBlob);
            return UploadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudBlob destBlob, UploadOptions options, TransferContext context)
        {
            return UploadAsync(sourceStream, destBlob, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure Blob Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destBlob">The <see cref="CloudBlob"/> that is the destination Azure blob.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        public static Task UploadAsync(Stream sourceStream, CloudBlob destBlob, UploadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceStream);
            TransferLocation destLocation = new TransferLocation(destBlob);
            return UploadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudFile destFile, UploadOptions options, TransferContext context)
        {
            return UploadAsync(sourcePath, destFile, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourcePath">Path to the source file.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(string sourcePath, CloudFile destFile, UploadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourcePath);
            TransferLocation destLocation = new TransferLocation(destFile);
            return UploadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudFile destFile, UploadOptions options, TransferContext context)
        {
            return UploadAsync(sourceStream, destFile, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Upload a file to Azure File Storage.
        /// </summary>
        /// <param name="sourceStream">A <see cref="System.IO.Stream"/> object providing the file content.</param>
        /// <param name="destFile">The <see cref="CloudFile"/> that is the destination Azure file.</param>
        /// <param name="options">An <see cref="UploadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task UploadAsync(Stream sourceStream, CloudFile destFile, UploadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceStream);
            TransferLocation destLocation = new TransferLocation(destFile);
            return UploadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, string destPath, DownloadOptions options, TransferContext context)
        {
            return DownloadAsync(sourceBlob, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, string destPath, DownloadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceBlob);
            TransferLocation destLocation = new TransferLocation(destPath);

            if (options != null)
            {
                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.RequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, Stream destStream, DownloadOptions options, TransferContext context)
        {
            return DownloadAsync(sourceBlob, destStream, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure blob from Azure Blob Storage.
        /// </summary>
        /// <param name="sourceBlob">The <see cref="CloudBlob"/> that is the source Azure blob.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudBlob sourceBlob, Stream destStream, DownloadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceBlob);
            TransferLocation destLocation = new TransferLocation(destStream);

            if (options != null)
            {
                BlobRequestOptions requestOptions = Transfer_RequestOptions.DefaultBlobRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.RequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, string destPath, DownloadOptions options, TransferContext context)
        {
            return DownloadAsync(sourceFile, destPath, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destPath">Path to the destination file.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, string destPath, DownloadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceFile);
            TransferLocation destLocation = new TransferLocation(destPath);

            if (options != null)
            {
                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.RequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, Stream destStream, DownloadOptions options, TransferContext context)
        {
            return DownloadAsync(sourceFile, destStream, options, context, CancellationToken.None);
        }

        /// <summary>
        /// Download an Azure file from Azure File Storage.
        /// </summary>
        /// <param name="sourceFile">The <see cref="CloudFile"/> that is the source Azure file.</param>
        /// <param name="destStream">A <see cref="System.IO.Stream"/> object representing the destination stream.</param>
        /// <param name="options">A <see cref="DownloadOptions"/> object that specifies additional options for the operation.</param>
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task DownloadAsync(CloudFile sourceFile, Stream destStream, DownloadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceFile);
            TransferLocation destLocation = new TransferLocation(destStream);

            if (options != null)
            {
                FileRequestOptions requestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;
                requestOptions.DisableContentMD5Validation = options.DisableContentMD5Validation;
                sourceLocation.RequestOptions = requestOptions;
            }

            return DownloadInternalAsync(sourceLocation, destLocation, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context)
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceBlob);
            TransferLocation destLocation = new TransferLocation(destBlob);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
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
        public static Task CopyAsync(CloudBlob sourceBlob, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context)
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudBlob sourceBlob, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceBlob);
            TransferLocation destLocation = new TransferLocation(destFile);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context)
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceFile);
            TransferLocation destLocation = new TransferLocation(destBlob);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context)
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        public static Task CopyAsync(CloudFile sourceFile, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            TransferLocation sourceLocation = new TransferLocation(sourceFile);
            TransferLocation destLocation = new TransferLocation(destFile);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure blob synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context)
        {
            return CopyAsync(sourceUri, destBlob, isServiceCopy, options, context, CancellationToken.None);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure blob synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudBlob destBlob, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            if (!isServiceCopy)
            {
                throw new NotSupportedException(Resources.SyncCopyFromUriToAzureBlobNotSupportedException);
            }

            TransferLocation sourceLocation = new TransferLocation(sourceUri);
            TransferLocation destLocation = new TransferLocation(destBlob);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure file synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context)
        {
            return CopyAsync(sourceUri, destFile, isServiceCopy, options, context, CancellationToken.None);
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
        /// <param name="context">A <see cref="TransferContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> object to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task"/> object that represents the asynchronous operation.</returns>
        /// <remarks>Copying from an URI to Azure file synchronously is not supported yet.</remarks>
        public static Task CopyAsync(Uri sourceUri, CloudFile destFile, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            if (!isServiceCopy)
            {
                throw new NotSupportedException(Resources.SyncCopyFromUriToAzureFileNotSupportedException);
            }

            TransferLocation sourceLocation = new TransferLocation(sourceUri);
            TransferLocation destLocation = new TransferLocation(destFile);
            return CopyInternalAsync(sourceLocation, destLocation, isServiceCopy, options, context, cancellationToken);
        }

        private static Task UploadInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, UploadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            if (options != null)
            {
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            Transfer transfer = CreateSingleObjectTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);
            if (options != null)
            {
                transfer.ContentType = options.ContentType;
            }

            return DoTransfer(transfer, cancellationToken);
        }

        private static Task DownloadInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, DownloadOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
            }

            Transfer transfer = CreateSingleObjectTransfer(sourceLocation, destLocation, TransferMethod.SyncCopy, context);
            return DoTransfer(transfer, cancellationToken);
        }

        private static Task CopyInternalAsync(TransferLocation sourceLocation, TransferLocation destLocation, bool isServiceCopy, CopyOptions options, TransferContext context, CancellationToken cancellationToken)
        {
            if (options != null)
            {
                sourceLocation.AccessCondition = options.SourceAccessCondition;
                destLocation.AccessCondition = options.DestinationAccessCondition;
            }

            Transfer transfer = CreateSingleObjectTransfer(sourceLocation, destLocation, isServiceCopy ? TransferMethod.AsyncCopy : TransferMethod.SyncCopy, context);
            return DoTransfer(transfer, cancellationToken);
        }

        private static async Task DoTransfer(Transfer transfer, CancellationToken cancellationToken)
        {
            if (!TryAddTransfer(transfer))
            {
                throw new TransferException(TransferErrorCode.TransferAlreadyExists, Resources.TransferAlreadyExists);
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

        private static Transfer CreateSingleObjectTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod, TransferContext transferContext)
        {
            Transfer transfer = GetTransfer(sourceLocation, destLocation, transferMethod, transferContext);
            if (transfer == null)
            {
                transfer = new SingleObjectTransfer(sourceLocation, destLocation, transferMethod);
                if (transferContext != null)
                {
                    transferContext.Checkpoint.AddTransfer(transfer);
                }
            }

            if (transferContext != null)
            {
                transfer.ProgressTracker.Parent = transferContext.OverallProgressTracker;
                transfer.Context = transferContext;
            }

            return transfer;
        }

        private static Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod, TransferContext transferContext)
        {
            Transfer transfer = null;
            if (transferContext != null)
            {
                transfer = transferContext.Checkpoint.GetTransfer(sourceLocation, destLocation, transferMethod);
                if (transfer != null)
                {
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
            if (targetLocation.TransferLocationType == TransferLocationType.AzureBlob)
            {
                targetLocation.UpdateCredentials(location.Blob.ServiceClient.Credentials);
            }
            else if (targetLocation.TransferLocationType == TransferLocationType.AzureFile)
            {
                targetLocation.UpdateCredentials(location.AzureFile.ServiceClient.Credentials);
            }
        }
    }
}
