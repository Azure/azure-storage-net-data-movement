//------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace S3ToAzureSample
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon;
    using Amazon.S3;
    using Amazon.S3.Model;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;

    /// <summary>
    /// This sample demonstrates how to copy all objects from an Amazon s3 bucket into a Microsoft Azure Blob container
    /// Before running this sample, you need to update it with your Amazon s3 and Microsoft Azure account settings.
    /// </summary>
    class Program
    {
        #region account settings
        // Amazon S3 account settings:
        // Change it to your aws access key id
        private static string AWSAccessKeyId = "[Your aws access key id]";

        // Change it to your aws access secret key
        private static string AWSSecretAccessKey = "[Your aws secret access key]";

        // Change it to the region your bucket is in
        private static RegionEndpoint S3BucketRegion = RegionEndpoint.USWest1;

        // Change it to your S3 bucket name to copy data from
        private static string S3BucketName = "[Your S3 bucket name]";

        // Microsoft Azure account settings:
        // Change it to your Azure Blob container name to copy data to
        private static string AzureContainerName = "[Your Azure Blob container name]";

        // Insert your Microsoft Azure Storage account name and account key into the following connection string
        private static string AzureConnectionString = "DefaultEndpointsProtocol=https;AccountName=[Your azure account];AccountKey=[Your azure account key]";

        #endregion

        // This sample uses two threads working as producer and consumer. This job queue is used to share data between two threads.
        private static BlockingCollection<TransferJob> jobQueue = new BlockingCollection<TransferJob>();

        // CountdownEvent to indicate the overall transfer completion
        private static CountdownEvent countdownEvent = new CountdownEvent(1);

        // A recorder helping to collect transfer summary from DataMovement library progress handler.
        private static ProgressRecorder progressRecorder = new ProgressRecorder();

        // Local folder used to store temporary files, it will be deleted after at the end of this sample
        private static string tempFolder = "TempFolder";

        // lock to block the console output when querying for user input
        private static object consoleLock = new object();

        // Amazon s3 client
        private static AmazonS3Client s3Client;

        // Microsoft Azure cloud blob client
        private static CloudBlobClient azureClient;

        static void Main(string[] args)
        {
            try
            {
                // Initialize the sample
                Init();

                ConsoleWriteLine("===Transfer begins===");

                // Start a thread to list objects from your Amazon s3 bucket
                Task.Run(() => { ListFromS3(); });

                // Start a thread to transfer listed objects into your Microsoft Azure Blob container
                Task.Run(() => { TransferToAzure(); });

                // Wait until all data are copied into Azure
                countdownEvent.Wait();

                ConsoleWriteLine("===Transfer finishes===");
                ConsoleWriteLine(progressRecorder.ToString());
            }
            finally
            {
                Cleanup();
            }
        }

        private static void Init()
        {
            // Create Amazon S3 client
            s3Client = GetS3Client();

            // Create Microsoft Azure client
            azureClient = GetAzureClient();

            // Create local temporary folder
            Directory.CreateDirectory(tempFolder);

            // Configue DataMovement library
            TransferManager.Configurations.UserAgentSuffix = "S3ToAzureSample";
        }

        private static void Cleanup()
        {
            // Delete the temporary folder
            Directory.Delete(tempFolder, true);
        }

        private static AmazonS3Client GetS3Client()
        {
            return new AmazonS3Client(AWSAccessKeyId, AWSSecretAccessKey, S3BucketRegion);
        }

        private static CloudBlobClient GetAzureClient()
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(AzureConnectionString);
            return account.CreateCloudBlobClient();
        }

        private static void ListFromS3()
        {
            AmazonS3Client client = GetS3Client();

            string previousMarker = null;
            bool listFinish = false;

            while (!listFinish)
            {
                ListObjectsRequest listObjectRequest = new ListObjectsRequest()
                {
                    BucketName = S3BucketName,
                    Marker = previousMarker,
                };

                ListObjectsResponse listObjectResponse = client.ListObjects(listObjectRequest);
                previousMarker = listObjectResponse.NextMarker;
                listFinish = String.IsNullOrEmpty(previousMarker);

                foreach (var source in listObjectResponse.S3Objects)
                {
                    ConsoleWriteLine("Object listed from bucket {0}: {1}", S3BucketName, source.Key);

                    // By default, the sample will download an amazon s3 object into a local file and
                    // then upload it into Microsoft Azure Stroage with DataMovement library later.
                    TransferJob job = CreateTransferJob(source);

                    // You can choose to use azure server side copy by replacing CreateTransferJob above
                    // with the following statement. When server side copy is used, Azure server will copy 
                    // the data directly from the URI provided and no data is downloaded to local.
                    // TransferJob job = CreateServiceSideTransferJob(source);

                    jobQueue.Add(job);
                }
            }

            jobQueue.CompleteAdding();
        }

        private static void TransferToAzure()
        {
            // Create the container if it doesn't exist yet
            CloudBlobClient client = azureClient;
            CloudBlobContainer container = client.GetContainerReference(AzureContainerName);
            container.CreateIfNotExists();

            TransferContext context = new TransferContext();

            // Add progress handler
            context.ProgressHandler = progressRecorder;

            context.OverwriteCallback = Program.OverwritePrompt;

            while(!jobQueue.IsCompleted)
            {
                TransferJob job = null;
                try
                {
                    job = jobQueue.Take();
                }
                catch(InvalidOperationException)
                {
                    // No more jobs to do
                }

                if (job == null)
                {
                    break;
                }

                countdownEvent.AddCount();

                CloudBlockBlob cloudBlob = container.GetBlockBlobReference(job.Name);

                ConsoleWriteLine("start to transfer {0} to azure.", job.Name);

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
                catch(Exception e)
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

        private static TransferJob CreateTransferJob(S3Object sourceObject)
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

                TransferJob job = new TransferJob()
                {
                    Source = tempFile,
                    Name = sourceObject.Key,
                    ServiceSideCopy = false
                };
                return job;
            }
        }

        private static TransferJob CreateServiceSideTransferJob(S3Object sourceObject)
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

            return new TransferJob()
            {
                Source = url,
                Name = sourceObject.Key,
                ServiceSideCopy = true,
            };
        }

        private static bool OverwritePrompt(string source, string destination)
        {
            lock (consoleLock)
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
            }
        }

        // Print message in console
        private static void ConsoleWriteLine(string format, params object[] args)
        {
            lock (consoleLock)
            {
                Console.WriteLine(format, args);
            }
        }

        // Print message in console
        private static void ConsoleWrite(string format, params object[] args)
        {
            lock (consoleLock)
            {
                Console.Write(format, args);
            }
        }
    }

    /// <summary>
    /// Entity class to represent a job to transfer from s3 to azure
    /// </summary>
    class TransferJob
    {
        public string Name;
        public string Source;
        public bool ServiceSideCopy;
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
            return string.Format("Transferred bytes: {0}; Transfered: {1}, Skipped: {2}, Failed: {3}",
                this.latestBytesTransferred,
                this.latestNumberOfFilesTransferred,
                this.latestNumberOfFilesSkipped,
                this.latestNumberOfFilesFailed);
        }
    }
}
