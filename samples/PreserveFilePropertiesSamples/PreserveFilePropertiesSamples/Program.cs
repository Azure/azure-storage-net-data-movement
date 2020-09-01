//------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace PreserveFilePropertiesSamples
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;

    static class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("The sample should be executed with elevated privileges.");
                Console.WriteLine("");
                Console.WriteLine("Samples to preserving file properties and permissions in blob uploading or downloading.");
                BlobDirectoryPreserveFilePropertiesSampleAsync().GetAwaiter().GetResult();
            }
            finally
            {
                Util.DeleteContainerAsync(ContainerName).GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Container name used in this sample.
        /// </summary>
        private const string ContainerName = "samplecontainer";

        /// <summary>
        /// Name of meta data to store file's creation time.
        /// </summary>
        private const string CreationTimeName = "CreationTime";

        /// <summary>
        /// Name of meta data to store file's last write time.
        /// </summary>
        private const string LastWriteTimeName = "LastWriteTime";

        /// <summary>
        /// Name of meta data to store file's attributes.
        /// </summary>
        private const string FileAttributesName = "FileAttributes";

        /// <summary>
        /// Name of meta data to store file's permissions.
        /// </summary>
        private const string FilePermissionsName = "FilePermissions";

        /// <summary>
        /// Upload local pictures to azure storage.
        ///   1. Upload png files starting with "azure" in the source directory as block blobs, not including the sub-directory.
        ///   2. Store source file's file attributes and permissions into destination blob's meta data.
        ///   3. Download png files starting with "azure" in the source directory to a local directory, not including the sub-directory.
        ///   4. Restore file attributes and permissions to destination local file.
        /// </summary>
        private static async Task BlobDirectoryPreserveFilePropertiesSampleAsync()
        {
            //Enable required privileges before getting/setting permissions from/to local file system.
            FileSecurityOperations.EnableRequiredPrivileges(PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL, true);

            try
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

                DirectoryTransferContext context = new DirectoryTransferContext();

                // Register for transfer event.
                context.FileTransferred += FileTransferredCallback;
                context.FileFailed += FileFailedCallback;
                context.FileSkipped += FileSkippedCallback;

                context.SetAttributesCallbackAsync = async (sourceObj, destination) =>
                {
                    string sourcePath = sourceObj as string;
                    DateTimeOffset? creationTime = null;
                    DateTimeOffset? lastWriteTime = null;
                    FileAttributes? fileAttributes = null;

                    FileOperations.GetFileProperties(sourcePath, out creationTime, out lastWriteTime, out fileAttributes);

                    string sourceFileSDDL = FileSecurityOperations.GetFilePortableSDDL(sourcePath,
                        PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL);

                    CloudBlob destBlob = destination as CloudBlob;

                    // Blob's original meta data has already been gotten from azure storage by DataMovement Library,
                    // Here only need to add new meta data key-value pairs, DataMovement Library will set these value to destination blob later.
                    destBlob.Metadata.Add(CreationTimeName, creationTime.Value.ToString());
                    destBlob.Metadata.Add(LastWriteTimeName, lastWriteTime.Value.ToString());
                    destBlob.Metadata.Add(FileAttributesName, fileAttributes.Value.ToString());
                    destBlob.Metadata.Add(FilePermissionsName, sourceFileSDDL);
                };

                context.ShouldTransferCallbackAsync = async (source, destination) =>
                {
                    // Can add more logic here to evaluate whether really need to transfer the target.
                    return true;
                };

                TransferStatus trasferStatus =
                    await TransferManager.UploadDirectoryAsync(sourceDirPath, destDir, options, context);


                Console.WriteLine("Final transfer state: {0}", TransferStatusToString(trasferStatus));
                Console.WriteLine("Files in directory {0} uploading to {1} is finished.", sourceDirPath, destDir.Uri.ToString());

                //Next the sample will show how to download a directory and restore file attributes to local file.
                string destDirPath = ".";
                CloudBlobDirectory sourceDir = await Util.GetCloudBlobDirectoryAsync(ContainerName, "blobdir");

                // In the following case, data movement library will download file named "azure.png" in the source directory,
                // not including the sub-directory.
                DownloadDirectoryOptions downloadDirectoryOptions = new DownloadDirectoryOptions()
                {
                    SearchPattern = "azure.png",
                    Recursive = false
                };

                DirectoryTransferContext directoryTransferContext = new DirectoryTransferContext();
                // Register for transfer event.
                directoryTransferContext.FileFailed += FileFailedCallback;
                directoryTransferContext.FileSkipped += FileSkippedCallback;

                //Get stored file properties from source blob meta data and set to local file.
                directoryTransferContext.FileTransferred += (object sender, TransferEventArgs e) =>
                {
                    CloudBlob sourceBlob = e.Source as CloudBlob;
                    string destFilePath = e.Destination as string;

                    string metadataValue = null;
                    DateTimeOffset creationTime = default(DateTimeOffset);
                    DateTimeOffset lastWriteTime = default(DateTimeOffset);
                    FileAttributes fileAttributes = default(FileAttributes);

                    bool gotCreationTime = false;
                    bool gotLastWriteTime = false;
                    bool gotFileAttributes = false;

                    if (sourceBlob.Metadata.TryGetValue(CreationTimeName, out metadataValue)
                        && !string.IsNullOrEmpty(metadataValue))
                    {
                        gotCreationTime = DateTimeOffset.TryParse(metadataValue, out creationTime);
                    }

                    if (sourceBlob.Metadata.TryGetValue(LastWriteTimeName, out metadataValue)
                        && !string.IsNullOrEmpty(metadataValue))
                    {
                        gotLastWriteTime = DateTimeOffset.TryParse(metadataValue, out lastWriteTime);
                    }

                    if (sourceBlob.Metadata.TryGetValue(FileAttributesName, out metadataValue)
                        && !string.IsNullOrEmpty(metadataValue))
                    {
                        gotFileAttributes = Enum.TryParse<FileAttributes>(metadataValue, out fileAttributes);
                    }

                    if (gotCreationTime && gotLastWriteTime && gotFileAttributes)
                    {
                        FileOperations.SetFileProperties(destFilePath, creationTime, lastWriteTime, fileAttributes);
                    }

                    if (sourceBlob.Metadata.TryGetValue(FilePermissionsName, out metadataValue)
                    && !string.IsNullOrEmpty(metadataValue))
                    {
                        FileSecurityOperations.SetFileSecurity(destFilePath, metadataValue,
                            PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL);
                    }

                };

                // Always writes to destination no matter it exists or not.
                directoryTransferContext.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;

                trasferStatus =
                    await TransferManager.DownloadDirectoryAsync(sourceDir, destDirPath, downloadDirectoryOptions, directoryTransferContext);


                Console.WriteLine("Final transfer state: {0}", TransferStatusToString(trasferStatus));
                Console.WriteLine("Files in directory {0} downloading to {1} is finished.", sourceDir.Uri.ToString(), destDirPath);
            }
            finally
            {
                //Restore privileges after getting/setting permissions from/to local file system.
                FileSecurityOperations.RestorePrivileges(PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL, true);
            }
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
    }
}
