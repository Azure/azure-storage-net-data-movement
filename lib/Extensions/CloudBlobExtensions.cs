// <copyright file="CloudBlobExtensions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferJobs;

    /// <summary>
    /// Defines extensions methods for ICloudBlob for use with BlobTransfer.
    /// </summary>
    public static class CloudBlobExtensions
    {
        /// <summary>
        /// Creates a job to start copying from a blob.
        /// </summary>
        /// <param name="destBlob">Destination blob to copy to. 
        /// User should call the method on this object.</param>
        /// <param name="sourceBlob">Source blob to copy from.</param>
        /// <returns>Job object to start copying.</returns>
        public static BlobStartCopyJob CreateStartCopyJob(
            this ICloudBlob destBlob,
            ICloudBlob sourceBlob)
        {
            return new BlobStartCopyJob()
            {
                SourceBlob = sourceBlob,
                DestBlob = destBlob
            };
        }

        /// <summary>
        /// Creates a job to start copying from a URI source.
        /// </summary>
        /// <param name="destBlob">Destination blob to copy to.
        /// User should call the method on this object.</param>
        /// <param name="sourceUri">Source to copy from.</param>
        /// <returns>Job object to start copying.</returns>
        public static BlobStartCopyJob CreateStartCopyJob(
            this ICloudBlob destBlob,
            Uri sourceUri)
        {
            return new BlobStartCopyJob()
            {
                SourceUri = sourceUri,
                DestBlob = destBlob
            };
        }

        /// <summary>
        /// Creates a job to copy from a blob.
        /// </summary>
        /// <param name="destBlob">Destination blob to copy to. 
        /// User should call the method on this object.</param>
        /// <param name="sourceBlob">Source blob to copy from.</param>
        /// <returns>Job object to do copying.</returns>
        public static BlobCopyJob CreateCopyJob(
            this ICloudBlob destBlob,
            ICloudBlob sourceBlob)
        {
            return new BlobCopyJob()
            {
                SourceBlob = sourceBlob,
                DestBlob = destBlob
            };
        }

        /// <summary>
        /// Creates a job to copy from a URI source.
        /// </summary>
        /// <param name="destBlob">Destination blob to copy to.
        /// User should call the method on this object.</param>
        /// <param name="sourceUri">Source to copy from.</param>
        /// <returns>Job object to do copying.</returns>
        public static BlobCopyJob CreateCopyJob(
            this ICloudBlob destBlob,
            Uri sourceUri)
        {
            return new BlobCopyJob()
            {
                SourceUri = sourceUri,
                DestBlob = destBlob                
            };
        }

        /// <summary>
        /// Creates a job to download a blob.
        /// </summary>
        /// <param name="sourceBlob">Source blob that to be downloaded.</param>
        /// <param name="destPath">Path of destination to download to.</param>
        /// <returns>Job instance to download blob.</returns>
        public static BlobDownloadJob CreateDownloadJob(
            this ICloudBlob sourceBlob,
            string destPath)
        {
            return new BlobDownloadJob()
            {
                SourceBlob = sourceBlob,
                DestPath = destPath
            };
        }

        /// <summary>
        /// Creates a job to download a blob.
        /// </summary>
        /// <param name="sourceBlob">Source blob that to be downloaded.</param>
        /// <param name="destStream">Destination stream to download to.</param>
        /// <returns>Job instance to download blob.</returns>
        public static BlobDownloadJob CreateDownloadJob(
            this ICloudBlob sourceBlob,
            Stream destStream)
        {
            return new BlobDownloadJob() 
            {
                SourceBlob = sourceBlob,
                DestStream = destStream
            };
        }

        /// <summary>
        /// Creates a job to upload a blob.
        /// </summary>
        /// <param name="destBlob">Destination blob to upload to.</param>
        /// <param name="sourcePath">Path of source file to upload from.</param>
        /// <returns>Job instance to upload blob.</returns>
        public static BlobUploadJob CreateUploadJob(
            this ICloudBlob destBlob,
            string sourcePath)
        {
            return new BlobUploadJob() 
            {
                SourcePath = sourcePath,
                DestBlob = destBlob
            };
        }
        
        /// <summary>
        /// Creates a job to upload a blob.
        /// </summary>
        /// <param name="destBlob">Destination blob to upload to.</param>
        /// <param name="sourceStream">Path of source file to upload from.</param>
        /// <returns>Job instance to upload blob.</returns>
        public static BlobUploadJob CreateUploadJob(
            this ICloudBlob destBlob,
            Stream sourceStream)
        {
            return new BlobUploadJob()
            {
                SourceStream = sourceStream,
                DestBlob = destBlob
            };
        }
    }
}
