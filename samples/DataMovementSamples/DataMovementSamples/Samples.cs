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
    using System.Runtime.Serialization;
#if !DOTNET5_4
    using System.Runtime.Serialization.Formatters.Binary;
#endif
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;

    public class Samples
    {
        public static void Main(string[] args)
        {
            try
            {
                // Single object transfer samples
                Console.WriteLine("Data movement upload sample.");
                BlobUploadSample().Wait();

                // Uncomment the following statement if the account supports Azure File Storage.
                // Note that Azure File Storage are not supported on Azure Storage Emulator.
                // FileUploadSample().Wait();

                // Azure Blob Storage are used in following copy and download samples. You can replace the 
                // source/destination object with CloudFile to transfer data from/to Azure File Storage as well.
                Console.WriteLine();
                Console.WriteLine("Data movement copy sample.");
                BlobCopySample().Wait();

                Console.WriteLine();
                Console.WriteLine("Data movement download sample.");
                BlobDownloadSample().Wait();

                Console.WriteLine();
                Console.WriteLine("Data movement download to stream sample");
                BlobDownloadToStreamSample().Wait();

                // Directory transfer samples
                Console.WriteLine();
                Console.WriteLine("Data movement directory upload sample");
                BlobDirectoryUploadSample().Wait();

                Console.WriteLine();
                Console.WriteLine("Data movement directory copy sample.");
                BlobDirectoryCopySample().Wait();
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
            // When transfer large file to block blob, set TransferManager.Configurations.BlockSize to specify the size of the blocks.
            // It must be between 4MB and 100MB and be multiple of 4MB. Default value is 4MB. 
            //
            // Currently, the max block count of a block blob is limited to 50000.
            // When transfering a big file and the BlockSize provided is smaller than the minimum value - (size/50000),
            // it'll be reset to a value which is greater than the minimum value and multiple of 4MB for this file.
            TransferManager.Configurations.BlockSize = 4 * 1024 * 1024; //4MB

            string sourceFileName = "azure.png";
            string destinationBlobName = "azure_blockblob.png";

            // Create the destination CloudBlob instance
            CloudBlob destinationBlob = await Util.GetCloudBlobAsync(ContainerName, destinationBlobName, BlobType.BlockBlob);

            // Use UploadOptions to set ContentType of destination CloudBlob
            UploadOptions options = new UploadOptions();

            SingleTransferContext context = new SingleTransferContext();
            context.SetAttributesCallbackAsync = async (destination) =>
            {
                CloudBlob destBlob = destination as CloudBlob;
                destBlob.Properties.ContentType = "image/png";
            };

            context.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;

            // Start the upload
            await TransferManager.UploadAsync(sourceFileName, destinationBlob, options, context);
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
            CloudFile destinationFile = await Util.GetCloudFileAsync(ShareName, destinationFileName);

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
            CloudBlob sourceBlob = await Util.GetCloudBlobAsync(ContainerName, sourceBlobName, BlobType.BlockBlob);

            // Create the destination CloudBlob instance
            CloudBlob destinationBlob = await Util.GetCloudBlobAsync(ContainerName, destinationBlobName, BlobType.BlockBlob);

            // Create CancellationTokenSource used to cancel the transfer
            CancellationTokenSource cancellationSource = new CancellationTokenSource();

            TransferCheckpoint checkpoint = null;
            SingleTransferContext context = new SingleTransferContext();

            // Start the transfer
            try
            {
                // With the CopyMethod parameter, you can indicate how the content would be copied to destination blob.
                // SyncCopy is to download source blob content to local memory and then upload to destination blob.
                // ServiceSideAsyncCopy is to send a start-copy request to Azure Storage Sever, and Azure Storage Server will do the actual copy.
                // ServiceSideSyncCopy will leverage REST API of Put Block From URL, Append Block From URL and Put Page From URL in Azure Storage Server.
                // Please see <c>https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url</c> for Put Block From URL,
                // <c>https://docs.microsoft.com/en-us/rest/api/storageservices/append-block-from-url</c> for Append Block From URL,
                // <c>https://docs.microsoft.com/en-us/rest/api/storageservices/put-page-from-url</c> for Put Page From URL.

                // Following will use ServiceSideSyncCopy to copy a blob.
                Task task = TransferManager.CopyAsync(sourceBlob, destinationBlob, CopyMethod.ServiceSideSyncCopy, null /* options */, context, cancellationSource.Token);

                // Sleep for 1 seconds and cancel the transfer. 
                // It may fail to cancel the transfer if transfer is done in 1 second. If so, no file will tranferred after resume.
                Thread.Sleep(1000);
                Console.WriteLine("Cancel the transfer.");
                cancellationSource.Cancel();

                await task;
            }
            catch (Exception e)
            {
                Console.WriteLine("The transfer is cancelled: {0}", e.Message);
            }

            // Store the transfer checkpoint
            checkpoint = context.LastCheckpoint;

            // Create a new TransferContext with the store checkpoint
            SingleTransferContext resumeContext = new SingleTransferContext(checkpoint);

            // Resume transfer from the stored checkpoint
            Console.WriteLine("Resume the cancelled transfer.");
            await TransferManager.CopyAsync(sourceBlob, destinationBlob, CopyMethod.ServiceSideSyncCopy, null /* options */, resumeContext);
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
            CloudBlob sourceBlob1 = await Util.GetCloudBlobAsync(ContainerName, sourceBlobName1, BlobType.BlockBlob);
            CloudBlob sourceBlob2 = await Util.GetCloudBlobAsync(ContainerName, sourceBlobName2, BlobType.BlockBlob);

            // Create a TransferContext shared by both transfers
            SingleTransferContext sharedTransferContext = new SingleTransferContext();

            // Show overwrite prompt in console when OverwriteCallback is triggered
            sharedTransferContext.ShouldOverwriteCallbackAsync = async (source, destination) =>
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
        /// Download data from Azure storage.
        ///   1. Download a CloudBlob to a Stream instance
        ///   2. Download the same CloudBlob with step #1 to a different Stream instance
        ///   3. Download another CloudBlob to a different stream with content MD5 validation disabled
        ///   4. Show the overall progress of all transfers
        /// </summary>
        private static async Task BlobDownloadToStreamSample()
        {
            string sourceBlobName1 = "azure_blockblob.png";
            string sourceBlobName2 = "azure_blockblob2.png";

            // Create the source CloudBlob instances
            CloudBlob sourceBlob1 = await Util.GetCloudBlobAsync(ContainerName, sourceBlobName1, BlobType.BlockBlob);
            CloudBlob sourceBlob2 = await Util.GetCloudBlobAsync(ContainerName, sourceBlobName2, BlobType.BlockBlob);

            // Create a TransferContext shared by both transfers
            SingleTransferContext sharedTransferContext = new SingleTransferContext();

            // Record the overall progress
            ProgressRecorder recorder = new ProgressRecorder();
            sharedTransferContext.ProgressHandler = recorder;

            MemoryStream memoryStream1_1 = new MemoryStream();
            MemoryStream memoryStream1_2 = new MemoryStream();
            MemoryStream memoryStream2 = new MemoryStream();

            try
            {
                // Start the blob download
                Task task1 = TransferManager.DownloadAsync(sourceBlob1, memoryStream1_1, null /* options */, sharedTransferContext);

                // Start to download the same blob to another Stream
                // Please note, DataMovement Library will download blob once for each of downloads.
                // For example, if you start two downloads from the same source blob to two different Stream instance, 
                // DataMovement Library will download the blob content twice.
                Task task2 = TransferManager.DownloadAsync(sourceBlob1, memoryStream1_2, null /* options */, sharedTransferContext);

                // Create a DownloadOptions to disable md5 check after data is downloaded. Otherwise, data movement 
                // library will check the md5 checksum stored in the ContentMD5 property of the source CloudFile/CloudBlob
                // You can uncomment following codes, enable ContentMD5Validation and have a try.
                //   sourceBlob2.Properties.ContentMD5 = "WrongMD5";
                //   sourceBlob2.SetProperties();
                DownloadOptions options = new DownloadOptions();
                options.DisableContentMD5Validation = true;

                // Start the download
                Task task3 = TransferManager.DownloadAsync(sourceBlob2, memoryStream2, options, sharedTransferContext);

                // Wait for all transfers to finish
                await task1;
                await task2;
                await task3;

                // Print out the final transfer state
                Console.WriteLine("Final transfer state: {0}", recorder.ToString());
            }
            finally
            {
                memoryStream1_1.Dispose();
                memoryStream1_2.Dispose();
                memoryStream2.Dispose();
            }
        }

        /// <summary>
        /// Upload local pictures to azure storage.
        ///   1. Upload png files starting with "azure" in the source directory as block blobs, not including the sub-directory
        ///      and store transfer context in a streamed journal.
        ///   2. Set their content type to "image/png".
        ///   3. Cancel the transfer before it finishes with a CancellationToken
        ///   3. Reload the transfer context from the streamed journal.
        ///   4. Resume the transfer with the loaded transfer context
        /// </summary>
        private static async Task BlobDirectoryUploadSample()
        {
            string sourceDirPath = ".";
            CloudBlobDirectory destDir = await Util.GetCloudBlobDirectoryAsync(ContainerName, "blobdir");

            // SearchPattern and Recuresive can be used to determine the files to be transferred from the source directory. The behavior of
            // SearchPattern and Recuresive varies for different source directory types.
            // See https://azure.github.io/azure-storage-net-data-movement for more details.
            //
            // When source is local directory, data movement library matches source files against the SearchPattern as standard wildcards. If 
            // recuresive is set to false, only files directly under the source directory will be matched. Otherwise, all files in the
            // sub-directory will be matched as well.
            //
            // In the following case, data movement library will upload png files starting with "azure" in the source directory as block blobs,
            // not including the sub-directory.
            UploadDirectoryOptions options = new UploadDirectoryOptions()
            {
                SearchPattern = "azure*.png",
                Recursive = false,
                BlobType = BlobType.BlockBlob
            };

            using (MemoryStream journalStream = new MemoryStream())
            {
                // Store the transfer context in a streamed journal.
                DirectoryTransferContext context = new DirectoryTransferContext(journalStream);

                // Register for transfer event.
                context.FileTransferred += FileTransferredCallback;
                context.FileFailed += FileFailedCallback;
                context.FileSkipped += FileSkippedCallback;

                context.SetAttributesCallbackAsync = async (destination) =>
                {
                    CloudBlob destBlob = destination as CloudBlob;
                    destBlob.Properties.ContentType = "image/png";
                };

                context.ShouldTransferCallbackAsync = async (source, destination) =>
                {
                    // Can add more logic here to evaluate whether really need to transfer the target.
                    return true;
                };

                // Create CancellationTokenSource used to cancel the transfer
                CancellationTokenSource cancellationSource = new CancellationTokenSource();

                TransferStatus trasferStatus = null;

                try
                {
                    // Start the upload
                    Task<TransferStatus> task = TransferManager.UploadDirectoryAsync(sourceDirPath, destDir, options, context, cancellationSource.Token);

                    // Sleep for 1 seconds and cancel the transfer. 
                    // It may fail to cancel the transfer if transfer is done in 1 second. If so, no file will be copied after resume.
                    Thread.Sleep(1000);
                    Console.WriteLine("Cancel the transfer.");
                    cancellationSource.Cancel();

                    trasferStatus = await task;
                }
                catch (Exception e)
                {
                    Console.WriteLine("The transfer is cancelled: {0}", e.Message);
                }

                journalStream.Position = 0;

                // Deserialize transfer context from the streamed journal.
                DirectoryTransferContext resumeContext = new DirectoryTransferContext(journalStream);
                resumeContext.FileTransferred += FileTransferredCallback;
                resumeContext.FileFailed += FileFailedCallback;
                resumeContext.FileSkipped += FileSkippedCallback;

                resumeContext.SetAttributesCallbackAsync = async (destination) =>
                {
                    CloudBlob destBlob = destination as CloudBlob;
                    destBlob.Properties.ContentType = "image/png";
                };

                resumeContext.ShouldTransferCallbackAsync = async (source, destination) =>
                {
                    // Can add more logic here to evaluate whether really need to transfer the target.
                    return true;
                };

                // Resume the upload
                trasferStatus = await TransferManager.UploadDirectoryAsync(sourceDirPath, destDir, options, resumeContext);

                Console.WriteLine("Final transfer state: {0}", TransferStatusToString(trasferStatus));
                Console.WriteLine("Files in directory {0} uploading to {1} is finished.", sourceDirPath, destDir.Uri.ToString());
            }
        }

        /// <summary>
        /// Copy data between Azure storage.
        ///   1. Copy a CloudBlobDirectory
        ///   2. Cancel the transfer before it finishes with a CancellationToken
        ///   3. Store the transfer checkpoint into a file after transfer being cancelled
        ///   4. Reload checkpoint from the file
        ///   4. Resume the transfer with the loaded checkpoint
        /// </summary>
        private static async Task BlobDirectoryCopySample()
        {
            CloudBlobDirectory sourceBlobDir = await Util.GetCloudBlobDirectoryAsync(ContainerName, "blobdir");
            CloudBlobDirectory destBlobDir = await Util.GetCloudBlobDirectoryAsync(ContainerName, "blobdir2");

            // When source is CloudBlobDirectory:
            //   1. If recursive is set to true, data movement library matches the source blob name against SearchPattern as prefix.
            //   2. Otherwise, data movement library matches the blob with the exact name specified by SearchPattern.
            //
            // You can also replace the source directory with a CloudFileDirectory instance to copy data from Azure File Storage. If so:
            //   1. If recursive is set to true, SearchPattern is not supported. Data movement library simply transfer all azure files
            //      under the source CloudFileDirectory and its sub-directories.
            //   2. Otherwise, data movement library matches the azure file with the exact name specified by SearchPattern.
            //
            // In the following case, data movement library will copy all blobs with the prefix "azure" in source blob directory.
            CopyDirectoryOptions options = new CopyDirectoryOptions()
            {
                SearchPattern = "azure",
                Recursive = true,
            };

            DirectoryTransferContext context = new DirectoryTransferContext();
            context.FileTransferred += FileTransferredCallback;
            context.FileFailed += FileFailedCallback;
            context.FileSkipped += FileSkippedCallback;

            // Create CancellationTokenSource used to cancel the transfer
            CancellationTokenSource cancellationSource = new CancellationTokenSource();

            TransferCheckpoint checkpoint = null;
            TransferStatus trasferStatus = null;

            try
            {
                Task<TransferStatus> task =  TransferManager.CopyDirectoryAsync(sourceBlobDir, destBlobDir, false /* isServiceCopy */, options, context, cancellationSource.Token);

                // Sleep for 1 seconds and cancel the transfer. 
                // It may fail to cancel the transfer if transfer is done in 1 second. If so, no file will be copied after resume.
                Thread.Sleep(1000);
                Console.WriteLine("Cancel the transfer.");
                cancellationSource.Cancel();

                trasferStatus = await task;
            }
            catch (Exception e)
            {
                Console.WriteLine("The transfer is cancelled: {0}", e.Message);
            }

            // Store the transfer checkpoint
            checkpoint = context.LastCheckpoint;

            // Serialize the checkpoint into a file
#if DOTNET5_4
            var formatter = new DataContractSerializer(typeof(TransferCheckpoint));
#else
            IFormatter formatter = new BinaryFormatter();
#endif

            string tempFileName = Guid.NewGuid().ToString();
            using (var stream = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                formatter.Serialize(stream, checkpoint);
            }

            // Deserialize the checkpoint from the file
            using (var stream = new FileStream(tempFileName, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                checkpoint = formatter.Deserialize(stream) as TransferCheckpoint;
            }

            File.Delete(tempFileName);

            // Create a new TransferContext with the store checkpoint
            DirectoryTransferContext resumeContext = new DirectoryTransferContext(checkpoint);
            resumeContext.FileTransferred += FileTransferredCallback;
            resumeContext.FileFailed += FileFailedCallback;
            resumeContext.FileSkipped += FileSkippedCallback;

            // Record the overall progress
            ProgressRecorder recorder = new ProgressRecorder();
            resumeContext.ProgressHandler = recorder;

            // Resume transfer from the stored checkpoint
            Console.WriteLine("Resume the cancelled transfer.");
            trasferStatus = await TransferManager.CopyDirectoryAsync(sourceBlobDir, destBlobDir, false /* isServiceCopy */, options, resumeContext);

            // Print out the final transfer state
            Console.WriteLine("Final transfer state: {0}", TransferStatusToString(trasferStatus));
        }

        private static void FileTransferredCallback(object sender, TransferEventArgs e)
        {
            Console.WriteLine("Transfer Succeeds. {0} -> {1}.", e.Source, e.Destination);
        }

        private static void FileFailedCallback(object sender, TransferEventArgs e)
        {
            Console.WriteLine("Transfer fails. {0} -> {1}. Error message:{2}", e.Source, e.Destination, e.Exception.Message);
        }

        private static void FileSkippedCallback(object sender, TransferEventArgs e)
        {
            Console.WriteLine("Transfer skips. {0} -> {1}.", e.Source, e.Destination);
        }

        /// <summary>
        /// Cleanup all data generated by this sample.
        /// </summary>
        private static void Cleanup()
        {
            Console.Write("Deleting container...");
            Util.DeleteContainerAsync(ContainerName).Wait();
            Console.WriteLine("Done");

            // Uncomment the following statements if the account supports Azure File Storage.
            // Console.Write("Deleting share...");
            // Util.DeleteShareAsync(ShareName).Wait();
            // Console.WriteLine("Done");

            // Delete the local file generated by download sample.
            Console.Write("Deleting local file...");
            File.Delete("azure_new.png");
            Console.WriteLine("Done");
        }

        /// <summary>
        /// Format the TransferStatus of DMlib to printable string 
        /// </summary>
        public static string TransferStatusToString(TransferStatus status)
        {
            return string.Format("Transferred bytes: {0}; Transfered: {1}; Skipped: {2}, Failed: {3}",
                status.BytesTransferred,
                status.NumberOfFilesTransferred,
                status.NumberOfFilesSkipped,
                status.NumberOfFilesFailed);
        }

        /// <summary>
        /// A helper class to record progress reported by data movement library.
        /// </summary>
        class ProgressRecorder : IProgress<TransferStatus>
        {
            private long latestBytesTransferred;
            private long latestNumberOfFilesTransferred;
            private long latestNumberOfFilesSkipped;
            private long latestNumberOfFilesFailed;

            public void Report(TransferStatus progress)
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
