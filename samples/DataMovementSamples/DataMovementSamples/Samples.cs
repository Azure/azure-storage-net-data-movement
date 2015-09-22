//------------------------------------------------------------------------------
// <copyright file="Samples.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DataMovementSamples
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

    public class Samples
    {
        public static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Data movement upload sample.");
                BlobUploadSample().Wait();

                // Uncomment the following statement if the account supports Azure File Storage.
                // Note that Azure File Storage are not supported on Azure Storage Emulator.
                // See http://azure.microsoft.com/en-us/services/preview/ to sign-up for on Azure Files Preview. 
                // FileUploadSample().Wait();

                // Azure Blob Storage are used in following copy and download samples. You can replace the 
                // source/destination object with CloudFile to transfer data from/to Azure File Storage as well.
                Console.WriteLine();
                Console.WriteLine("Data movement copy sample.");
                BlobCopySample().Wait();

                Console.WriteLine();
                Console.WriteLine("Data movement download sample.");
                BlobDownloadSample().Wait();
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Cleanup generated data.");
                Cleanup();
            }
        }

        /// <summary>
        /// Container name used in this sample.
        /// </summary>
        private const string ContainerName = "samplecontainer";

        /// <summary>
        /// Share name used in this sample.
        /// </summary>
        private const string ShareName = "sampleshare";

        /// <summary>
        /// Upload a local picture to azure storage.
        ///   1. Upload a local picture as a block blob.
        ///   2. Set its content type to "image/png".
        /// </summary>
        private static async Task BlobUploadSample()
        {
            string sourceFileName = "azure.png";
            string destinationBlobName = "azure_blockblob.png";

            // Create the destination CloudBlob instance
            CloudBlob destinationBlob = Util.GetCloudBlob(ContainerName, destinationBlobName, BlobType.BlockBlob);

            // Use UploadOptions to set ContentType of destination CloudBlob
            UploadOptions options = new UploadOptions();
            options.ContentType = "image/png";

            // Start the upload
            await TransferManager.UploadAsync(sourceFileName, destinationBlob, options, null /* context */);
            Console.WriteLine("File {0} is uploaded to {1} successfully.", sourceFileName, destinationBlob.Uri.ToString());
        }

        /// <summary>
        /// Upload a local picture to azure storage as a cloud file.
        /// </summary>
        private static async Task FileUploadSample()
        {
            string sourceFileName = "azure.png";
            string destinationFileName = "azure_cloudfile.png";

            // Create the destination CloudFile instance
            CloudFile destinationFile = Util.GetCloudFile(ShareName, destinationFileName);

            // Start the upload
            await TransferManager.UploadAsync(sourceFileName, destinationFile);
            Console.WriteLine("File {0} is uploaded to {1} successfully.", sourceFileName, destinationFile.Uri.ToString());
        }

        /// <summary>
        /// Copy data between Azure storage.
        ///   1. Copy a CloudBlob
        ///   2. Cancel the transfer before it finishes with a CancellationToken
        ///   3. Store the transfer checkpoint after transfer being cancelled
        ///   4. Resume the transfer with the stored checkpoint
        /// </summary>
        private static async Task BlobCopySample()
        {
            string sourceBlobName = "azure_blockblob.png";
            string destinationBlobName = "azure_blockblob2.png";

            // Create the source CloudBlob instance
            CloudBlob sourceBlob = Util.GetCloudBlob(ContainerName, sourceBlobName, BlobType.BlockBlob);

            // Create the destination CloudBlob instance
            CloudBlob destinationBlob = Util.GetCloudBlob(ContainerName, destinationBlobName, BlobType.BlockBlob);

            // Create CancellationTokenSource used to cancel the transfer
            CancellationTokenSource cancellationSource = new CancellationTokenSource();

            TransferCheckpoint checkpoint = null;
            TransferContext context = new TransferContext();

            // Cancel the transfer after there's any progress reported
            Progress<TransferProgress> progress = new Progress<TransferProgress>(
                (transferProgress) => {
                    if (!cancellationSource.IsCancellationRequested)
                    {
                        Console.WriteLine("Cancel the transfer.");

                        // Cancel the transfer
                        cancellationSource.Cancel();
                    }
                });

            context.ProgressHandler = progress;

            // Start the transfer
            try
            {
                await TransferManager.CopyAsync(sourceBlob, destinationBlob, false /* isServiceCopy */, null /* options */, context, cancellationSource.Token);
            }
            catch (Exception e)
            {
                Console.WriteLine("The transfer is cancelled: {0}", e.Message);
            }

            // Store the transfer checkpoint
            checkpoint = context.LastCheckpoint;

            // Create a new TransferContext with the store checkpoint
            TransferContext resumeContext = new TransferContext(checkpoint);

            // Resume transfer from the stored checkpoint
            Console.WriteLine("Resume the cancelled transfer.");
            await TransferManager.CopyAsync(sourceBlob, destinationBlob, false /* isServiceCopy */, null /* options */, resumeContext);
            Console.WriteLine("CloudBlob {0} is copied to {1} successfully.", sourceBlob.Uri.ToString(), destinationBlob.Uri.ToString());
        }

        /// <summary>
        /// Download data from Azure storage.
        ///   1. Download a CloudBlob to an exsiting local file
        ///   2. Query the user to overwrite the local file or not in the OverwriteCallback
        ///   3. Download another CloudBlob to local with content MD5 validation disabled
        ///   4. Show the overall progress of both transfers
        /// </summary>
        private static async Task BlobDownloadSample()
        {
            string sourceBlobName1 = "azure_blockblob.png";
            string sourceBlobName2 = "azure_blockblob2.png";
            string destinationFileName1 = "azure.png";
            string destinationFileName2 = "azure_new.png";

            // Create the source CloudBlob instances
            CloudBlob sourceBlob1 = Util.GetCloudBlob(ContainerName, sourceBlobName1, BlobType.BlockBlob);
            CloudBlob sourceBlob2 = Util.GetCloudBlob(ContainerName, sourceBlobName2, BlobType.BlockBlob);

            // Create a TransferContext shared by both transfers
            TransferContext sharedTransferContext = new TransferContext();

            // Show overwrite prompt in console when OverwriteCallback is triggered
            sharedTransferContext.OverwriteCallback = (source, destination) =>
                {
                    Console.WriteLine("{0} already exists. Do you want to overwrite it with {1}? (Y/N)", destination, source);

                    while (true)
                    {
                        ConsoleKeyInfo keyInfo = Console.ReadKey(true);
                        char key = keyInfo.KeyChar;

                        if (key == 'y' || key == 'Y')
                        {
                            Console.WriteLine("User choose to overwrite the destination.");
                            return true;
                        }
                        else if (key == 'n' || key == 'N')
                        {
                            Console.WriteLine("User choose NOT to overwrite the destination.");
                            return false;
                        }

                        Console.WriteLine("Please press 'y' or 'n'.");
                    }
                };

            // Record the overall progress
            ProgressRecorder recorder = new ProgressRecorder();
            sharedTransferContext.ProgressHandler = recorder;

            // Start the blob download
            Task task1 = TransferManager.DownloadAsync(sourceBlob1, destinationFileName1, null /* options */, sharedTransferContext);

            // Create a DownloadOptions to disable md5 check after data is downloaded. Otherwise, data movement 
            // library will check the md5 checksum stored in the ContentMD5 property of the source CloudFile/CloudBlob
            // You can uncomment following codes, enable ContentMD5Validation and have a try.
            //   sourceBlob2.Properties.ContentMD5 = "WrongMD5";
            //   sourceBlob2.SetProperties();
            DownloadOptions options = new DownloadOptions();
            options.DisableContentMD5Validation = true;
            
            // Start the download
            Task task2 = TransferManager.DownloadAsync(sourceBlob2, destinationFileName2, options, sharedTransferContext);

            // Wait for both transfers to finish
            try
            {
                await task1;
            }
            catch(Exception e)
            {
                // Data movement library will throw a TransferException when user choose to not overwrite the existing destination
                Console.WriteLine(e.Message);
            }

            await task2;

            // Print out the final transfer state
            Console.WriteLine("Final transfer state: {0}", recorder.ToString());
        }

        /// <summary>
        /// Cleanup all data generated by this sample.
        /// </summary>
        private static void Cleanup()
        {
            Console.Write("Deleting container...");
            Util.DeleteContainer(ContainerName);
            Console.WriteLine("Done");

            // Uncomment the following statements if the account supports Azure File Storage.
            // Console.Write("Deleting share...");
            // Util.DeleteShare(ShareName);
            // Console.WriteLine("Done");

            // Delete the local file generated by download sample.
            Console.Write("Deleting local file...");
            File.Delete("azure_new.png");
            Console.WriteLine("Done");
        }

        /// <summary>
        /// A helper class to record progress reported by data movement library.
        /// </summary>
        class ProgressRecorder : IProgress<TransferProgress>
        {
            private long latestBytesTransferred;
            private long latestNumberOfFilesTransferred;
            private long latestNumberOfFilesSkipped;
            private long latestNumberOfFilesFailed;

            public void Report(TransferProgress progress)
            {
                this.latestBytesTransferred = progress.BytesTransferred;
                this.latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
                this.latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
                this.latestNumberOfFilesFailed = progress.NumberOfFilesFailed;
            }

            public override string ToString()
            {
                return string.Format("Transferred bytes: {0}; Transfered: {1}; Skipped: {2}, Failed: {3}",
                    this.latestBytesTransferred,
                    this.latestNumberOfFilesTransferred,
                    this.latestNumberOfFilesSkipped,
                    this.latestNumberOfFilesFailed);
            }
        }
    }
}
