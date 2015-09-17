// <copyright file="CloudFileExtensions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferJobs;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Defines extensions methods for CloudFile to create FileTransferJobs.
    /// </summary>
    public static class CloudFileExtensions
    {
        /// <summary>
        /// Creates a job to download a cloud file.
        /// </summary>
        /// <param name="sourceFile">Source file that to be downloaded.</param>
        /// <param name="destPath">Path of destination to download to.</param>
        /// <returns>Job instance to download file.</returns>
        public static FileDownloadJob CreateDownloadJob(
            this CloudFile sourceFile,
            string destPath)
        {
            return new FileDownloadJob()
            {
                SourceFile = sourceFile,
                DestPath = destPath
            };
        }

        /// <summary>
        /// Creates a job to download a cloud file.
        /// </summary>
        /// <param name="sourceFile">Source file that to be downloaded.</param>
        /// <param name="destStream">Destination stream to download to.</param>
        /// <returns>Job instance to download file.</returns>
        public static FileDownloadJob CreateDownloadJob(
            this CloudFile sourceFile,
            Stream destStream)
        {
            return new FileDownloadJob()
            {
                SourceFile = sourceFile,
                DestStream = destStream
            };
        }

        /// <summary>
        /// Creates a job to upload a cloud file.
        /// </summary>
        /// <param name="destFile">Destination file to upload to.</param>
        /// <param name="sourcePath">Path of source file to upload from.</param>
        /// <returns>Job instance to upload file.</returns>
        public static FileUploadJob CreateUploadJob(
            this CloudFile destFile,
            string sourcePath)
        {
            return new FileUploadJob()
            {
                DestFile = destFile,
                SourcePath = sourcePath
            };
        }

        /// <summary>
        /// Creates a job to upload a cloud file.
        /// </summary>
        /// <param name="destFile">Destination file to upload to.</param>
        /// <param name="sourceStream">Path of source file to upload from.</param>
        /// <returns>Job instance to upload file.</returns>
        public static FileUploadJob CreateUploadJob(
            this CloudFile destFile,
            Stream sourceStream)
        {
            return new FileUploadJob()
            {
                DestFile = destFile,
                SourceStream = sourceStream
            };
        }

        /// <summary>
        /// Creates a job to delete a cloud file.
        /// </summary>
        /// <param name="fileToDelete">File to delete.</param>
        /// <returns>Job instance to delete file.</returns>
        public static FileDeleteJob CreateDeleteJob(
            this CloudFile fileToDelete)
        {
            return new FileDeleteJob()
            {
                File = fileToDelete
            };
        }
    }
}
