//------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace S3ToAzureSample
{
    using System;
    using System.Collections.Concurrent;
    using System.Configuration;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon;
    using Amazon.S3;
    using Amazon.S3.Model;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// This sample demonstrates how to copy all objects from an Amazon s3 bucket into a Microsoft Azure blob container
    /// Before running this sample, you need to update it with your Amazon s3 and Microsoft Azure account settings.
    /// </summary>
    static class Program
    {
        #region account settings
        // Please set your account credentials in app.config.

        // Change it to the region your S3 bucket is in.
        private static readonly RegionEndpoint S3BucketRegion = RegionEndpoint.USWest1;

        // Change it to your S3 bucket name to copy data from.
        private static readonly string S3BucketName = "[Your S3 bucket name]";

        // Change it to your Azure blob container name to copy data to.
        private static readonly string AzureContainerName = "[Your Azure blob container name]";

        #endregion

        // This sample uses two threads working as producer and consumer. This job queue is used to share data between two threads.
        private static BlockingCollection<S3ToAzureTransferJob> jobQueue = new BlockingCollection<S3ToAzureTransferJob>();

        // CountdownEvent to indicate the overall transfer completion.
        private static CountdownEvent countdownEvent = new CountdownEvent(1);

        // A recorder helping to collect transfer summary from DataMovement library progress handler.
        private static ProgressRecorder progressRecorder = new ProgressRecorder();

        // Local folder used to store temporary files, it will be deleted after at the end of this sample.
        private static string tempFolder = "TempFolder";

        // lock to block the console output when querying for user input.
        private static object consoleLock = new object();

        // Amazon s3 client.
        private static AmazonS3Client s3Client;

        // Microsoft Azure cloud blob client.
        private static CloudBlobClient azureClient;

        static void Main(string[] args)
        {
            try
            {
                // Create Amazon S3 client
                string awsAccessKeyId = LoadSettingFromAppConfig("AWSAccessKeyId");
                string awsSecretAccessKey = LoadSettingFromAppConfig("AWSSecretAccessKey");
                s3Client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, S3BucketRegion);

                // Create Microsoft Azure client
                string azureConnectionString = LoadSettingFromAppConfig("AzureStorageConnectionString");
                CloudStorageAccount account = CloudStorageAccount.Parse(azureConnectionString);
                azureClient = account.CreateCloudBlobClient();

                // Create local temporary folder
                Directory.CreateDirectory(tempFolder);

                // Configue DataMovement library
                TransferManager.Configurations.UserAgentPrefix = "S3ToAzureSample";

                ConsoleWriteLine("===Transfer begins===");

                // Start a thread to list objects from your Amazon s3 bucket
                Task.Run(() => { ListFromS3(); });

                // Start a thread to transfer listed objects into your Microsoft Azure blob container
                Task.Run(() => { TransferToAzure(); });

                // Wait until all data are copied into Azure
                countdownEvent.Wait();

                ConsoleWriteLine("===Transfer finishes===");
                ConsoleWriteLine(progressRecorder.ToString());
            }
            finally
            {
                // Delete the temporary folder
                Directory.Delete(tempFolder, true);
            }
        }

        /// <summary>
        /// Load a setting from app.config.
        /// </summary>
        /// <param name="key">Key of setting.</param>
        /// <returns>Value of setting.</returns>
        private static string LoadSettingFromAppConfig(string key)
        {
            string result = ConfigurationManager.AppSettings[key];

            if (string.IsNullOrEmpty(result))
            {
                throw new InvalidOperationException(string.Format("{0} is not set in App.config.", key));
            }

            return result;
        }

        /// <summary>
        /// List all objects from your S3 bucket and add one job into jobQueue for each listed object.
        /// </summary>
        private static void ListFromS3()
        {
            string previousMarker = null;
            bool listFinish = false;

            while (!listFinish)
            {
                ListObjectsRequest listObjectRequest = new ListObjectsRequest()
                {
                    BucketName = S3BucketName,
                    Marker = previousMarker,
                };

                ListObjectsResponse listObjectResponse = s3Client.ListObjects(listObjectRequest);
                previousMarker = listObjectResponse.NextMarker;
                listFinish = String.IsNullOrEmpty(previousMarker);

                foreach (var source in listObjectResponse.S3Objects)
                {
                    ConsoleWriteLine("Object listed from bucket {0}: {1}", S3BucketName, source.Key);

                    // By default, the sample will download an amazon s3 object into a local file and
                    // then upload it into Microsoft Azure Stroage with DataMovement library later.
                    S3ToAzureTransferJob job = CreateTransferJob(source);

                    // You can choose to use azure server side copy by replacing CreateTransferJob above
                    // with the following statement. When server side copy is used, Azure server will copy 
                    // the data directly from the URI provided and no data is downloaded to local.
                    // TransferJob job = CreateServiceSideTransferJob(source);

                    jobQueue.Add(job);
                }
            }

            jobQueue.CompleteAdding();
        }

        /// <summary>
        /// Get job from jobQueue and transfer data into your Azure blob container.
        /// </summary>
        private static void TransferToAzure()
        {
            // Create the container if it doesn't exist yet
            CloudBlobClient client = azureClient;
            CloudBlobContainer container = client.GetContainerReference(AzureContainerName);
            container.CreateIfNotExists();

            SingleTransferContext context = new SingleTransferContext();

            // Add progress handler
            context.ProgressHandler = progressRecorder;

            context.ShouldOverwriteCallbackAsync = Program.OverwritePrompt;

            while (!jobQueue.IsCompleted)
            {
                S3ToAzureTransferJob job = null;
                try
                {
                    job = jobQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    // No more jobs to do
                }

                if (job == null)
                {
                    break;
                }

                countdownEvent.AddCount();

                CloudBlockBlob cloudBlob = container.GetBlockBlobReference(job.Name);

                ConsoleWriteLine("Start to transfer {0} to azure.", job.Name);

                Task task = null;

                try
                {
                    if (!job.ServiceSideCopy)
                    {
                        // By default, the sample will download an amazon s3 object into a local file and
                        // then upload it into Microsoft Azure Stroage with DataMovement library.
                        task = TransferManager.UploadAsync(job.Source, cloudBlob, null, context);

                    }
                    else
                    {
                        // When server side copy is used, Azure server will copy the data directly from the URI 
                        // provided and no data is downloaded to local.
                        task = TransferManager.CopyAsync(new Uri(job.Source), cloudBlob, true, null, context);
                    }
                }
                catch (Exception e)
                {
                    ConsoleWriteLine("Error occurs when transferring {0}: {1}", job.Name, e.ToString());
                }

                if (task != null)
                {
                    task.ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            ConsoleWriteLine("Error occurs when transferring {0}: {1}", job.Name, t.Exception.ToString());
                        }
                        else
                        {
                            ConsoleWriteLine("Succeed to transfer data to blob {0}", job.Name);
                        }

                        // Signal the countdown event when one transfer job finishes.
                        countdownEvent.Signal();
                    });
                }
                else
                {
                    // Signal the countdown event when one transfer job finishes.
                    countdownEvent.Signal();
                }
            }

            // Signal the countdown event to unblock the main thread when all data are transferred.
            countdownEvent.Signal();
        }

        /// <summary>
        /// Create a <see cref="S3ToAzureTransferJob"/> representing a download-to-local-and-upload copy from one S3 object to Azure blob.
        /// </summary>
        /// <param name="sourceObject">S3 object used to create the job.</param>
        /// <returns>A job representing a download-to-local-and-upload copy from one S3 object to Azure blob.</returns>
        private static S3ToAzureTransferJob CreateTransferJob(S3Object sourceObject)
        {
            // Download the source object to a temporary file
            GetObjectRequest getObjectRequest = new GetObjectRequest()
            {
                BucketName = S3BucketName,
                Key = sourceObject.Key,
            };

            using (GetObjectResponse getObjectResponse = s3Client.GetObject(getObjectRequest))
            {
                string tempFile = Path.Combine(tempFolder, Guid.NewGuid().ToString());
                getObjectResponse.WriteResponseStreamToFile(tempFile);

                S3ToAzureTransferJob job = new S3ToAzureTransferJob()
                {
                    Source = tempFile,
                    Name = sourceObject.Key,
                    ServiceSideCopy = false
                };

                return job;
            }
        }

        /// <summary>
        /// Create a <see cref="S3ToAzureTransferJob"/> representing a service side copy from one S3 object to Azure blob.
        /// </summary>
        /// <param name="sourceObject">S3 object used to create the job.</param>
        /// <returns>A job representing a service side copy from one S3 object to Azure blob</returns>
        private static S3ToAzureTransferJob CreateServiceSideTransferJob(S3Object sourceObject)
        {
            // Azure server side copy requires read permission of the source data
            // Generate a pre-signed url for the amazon s3 object here
            GetPreSignedUrlRequest getPresignedUrlRequest = new GetPreSignedUrlRequest()
            {
                BucketName = S3BucketName,
                Key = sourceObject.Key,
                Expires = DateTime.Now.AddMinutes(10)
            };

            string url = s3Client.GetPreSignedURL(getPresignedUrlRequest);

            return new S3ToAzureTransferJob()
            {
                Source = url,
                Name = sourceObject.Key,
                ServiceSideCopy = true,
            };
        }

        /// <summary>
        /// Overwrite callback used in <see cref="Microsoft.WindowsAzure.Storage.DataMovement.TransferContext"/> to query user if overwrite
        /// an existing destination blob.
        /// </summary>
        /// <param name="source">Instance of source used to overwrite the destination.</param>
        /// <param name="destination">Instance of the destination to be overwritten.</param>
        /// <returns>True if the destination should be overwritten; otherwise false.</returns>
        private static async Task<bool> OverwritePrompt(object source, object destination)
        {
            return await Task<bool>.Run(() =>
            {
                lock (consoleLock)
                {
                    Console.WriteLine("{0} already exists. Do you want to overwrite it with {1}? (Y/N)", ToString(destination), ToString(source));

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
                }
            });
        }

        /// <summary>
        /// Print message in console. Will be blocked while querying for user input.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array of objects to write using format.</param>
        private static void ConsoleWriteLine(string format, params object[] args)
        {
            lock (consoleLock)
            {
                Console.WriteLine(format, args);
            }
        }

        /// <summary>
        /// Print message in console. Will be blocked while querying for user input.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An array of objects to write using format.</param>
        private static void ConsoleWrite(string format, params object[] args)
        {
            lock (consoleLock)
            {
                Console.Write(format, args);
            }
        }

        public static string ToString(object transferTarget)
        {
            CloudBlob blob = transferTarget as CloudBlob;

            if (null != blob)
            {
                return blob.SnapshotQualifiedUri.AbsoluteUri;
            }

            CloudFile file = transferTarget as CloudFile;

            if (null != file)
            {
                return file.Uri.AbsoluteUri;
            }

            return transferTarget.ToString();
        }
    }

    /// <summary>
    /// Entity class to represent a job to transfer from s3 to azure
    /// </summary>
    class S3ToAzureTransferJob
    {
        public string Name;
        public string Source;
        public bool ServiceSideCopy;
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

        /// <summary>
        /// Callback to get the progress from data movement library.
        /// </summary>
        /// <param name="progress">Transfer progress.</param>
        public void Report(TransferStatus progress)
        {
            this.latestBytesTransferred = progress.BytesTransferred;
            this.latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
            this.latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
            this.latestNumberOfFilesFailed = progress.NumberOfFilesFailed;
        }

        /// <summary>
        /// Return the recorded progress information.
        /// </summary>
        /// <returns>Recorded progress information.</returns>
        public override string ToString()
        {
            return string.Format("Transferred bytes: {0}; Transfered: {1}, Skipped: {2}, Failed: {3}",
                this.latestBytesTransferred,
                this.latestNumberOfFilesTransferred,
                this.latestNumberOfFilesSkipped,
                this.latestNumberOfFilesFailed);
        }
    }
}
