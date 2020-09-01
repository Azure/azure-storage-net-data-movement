//------------------------------------------------------------------------------
// <copyright file="Util.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace PreserveFilePropertiesSamples
{
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A helper class provides convenient operations against storage account configured in the App.config.
    /// </summary>
    public class Util
    {
        private static CloudStorageAccount storageAccount;
        private static CloudBlobClient blobClient;

        /// <summary>
        /// Get a CloudBlobDirectory instance with the specified name in the given container.
        /// </summary>
        /// <param name="containerName">Container name.</param>
        /// <param name="directoryName">Blob directory name.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="CloudBlobDirectory"/> that represents the asynchronous operation.</returns>
        public static async Task<CloudBlobDirectory> GetCloudBlobDirectoryAsync(string containerName, string directoryName)
        {
            CloudBlobClient client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            return container.GetDirectoryReference(directoryName);
        }

        /// <summary>
        /// Delete the container with the specified name if it exists.
        /// </summary>
        /// <param name="containerName">Name of container to delete.</param>
        public static async Task DeleteContainerAsync(string containerName)
        {
            CloudBlobClient client = GetCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            await container.DeleteIfExistsAsync();
        }

        private static CloudBlobClient GetCloudBlobClient()
        {
            if (Util.blobClient == null)
            {
                Util.blobClient = GetStorageAccount().CreateCloudBlobClient();
            }

            return Util.blobClient;
        }

        private static string LoadConnectionStringFromConfigration()
        {
            // How to create a storage connection string: http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
#if DOTNET5_4
            //For .Net Core,  will get Storage Connection string from Config.json file
            return JObject.Parse(File.ReadAllText("Config.json"))["StorageConnectionString"].ToString(); 
#else
            //For .net, will get Storage Connection string from App.Config file
            return System.Configuration.ConfigurationManager.AppSettings["StorageConnectionString"];
#endif
        }

        private static CloudStorageAccount GetStorageAccount()
        {
            if (Util.storageAccount == null)
            {
                string connectionString = LoadConnectionStringFromConfigration();
                Util.storageAccount = CloudStorageAccount.Parse(connectionString);
            }

            return Util.storageAccount;
        }
    }
}
