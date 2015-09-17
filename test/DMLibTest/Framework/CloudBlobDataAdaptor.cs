//------------------------------------------------------------------------------
// <copyright file="CloudBlobDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using StorageBlob = Microsoft.WindowsAzure.Storage.Blob;

    internal class CloudBlobDataAdaptor : BlobDataAdaptorBase<DMLibDataInfo>
    {
        public CloudBlobDataAdaptor(TestAccount testAccount, string containerName, string blobType, SourceOrDest sourceOrDest, string delimiter = "/")
            : base(testAccount, containerName, blobType, sourceOrDest, delimiter)
        {
        }

        public override object GetTransferObject(FileNode fileNode)
        {
            return this.GetCloudBlobReference(fileNode);
        }

        public override object GetTransferObject(DirNode dirNode)
        {
            return this.GetCloudBlobDirReference(dirNode);
        }

        protected override void GenerateDataImp(DMLibDataInfo dataInfo)
        {
            this.BlobHelper.CreateContainer(this.ContainerName);

            using (TemporaryTestFolder localTemp = new TemporaryTestFolder(this.TempFolder))
            {
                CloudBlobDirectory rootCloudBlobDir = this.BlobHelper.GetDirReference(this.ContainerName, dataInfo.RootPath);
                this.GenerateDir(dataInfo.RootNode, rootCloudBlobDir);
            }
        }
        
        public override DMLibDataInfo GetTransferDataInfo(string rootDir)
        {
            CloudBlobDirectory blobDir = this.BlobHelper.QueryBlobDirectory(this.ContainerName, rootDir);
            if (blobDir == null)
            {
                return null;
            }

            DMLibDataInfo dataInfo = new DMLibDataInfo(rootDir);

            this.BuildDirNode(blobDir, dataInfo.RootNode);
            return dataInfo;
        }

        public string LeaseBlob(FileNode fileNode, TimeSpan? leaseTime)
        {
            var blob = this.GetCloudBlobReference(fileNode);
            return blob.AcquireLease(leaseTime, null, options: HelperConst.DefaultBlobOptions);
        }

        public void ReleaseLease(FileNode fileNode, string leaseId)
        {
            var blob = this.GetCloudBlobReference(fileNode);
            blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId), options: HelperConst.DefaultBlobOptions);
        }

        public CloudBlob GetCloudBlobReference(FileNode fileNode)
        {
            var container = this.BlobHelper.BlobClient.GetContainerReference(this.ContainerName);
            var blobName = fileNode.GetURLRelativePath();
            if (blobName.StartsWith("/"))
            {
                blobName = blobName.Substring(1, blobName.Length - 1);
            }

            return CloudBlobHelper.GetCloudBlobReference(container, blobName, this.BlobType);
        }

        public CloudBlobDirectory GetCloudBlobDirReference(DirNode dirNode)
        {
            var container = this.BlobHelper.BlobClient.GetContainerReference(this.ContainerName);
            var dirName = dirNode.GetURLRelativePath();
            if (dirName.StartsWith("/"))
            {
                dirName = dirName.Substring(1, dirName.Length - 1);
            }

            return container.GetDirectoryReference(dirName);
        }

        private void GenerateDir(DirNode dirNode, CloudBlobDirectory cloudBlobDir)
        {
            DMLibDataHelper.CreateLocalDirIfNotExists(this.TempFolder);

            foreach (var subDir in dirNode.DirNodes)
            {
                CloudBlobDirectory subCloudBlobDir = cloudBlobDir.GetDirectoryReference(subDir.Name);
                this.GenerateDir(subDir, subCloudBlobDir);
            }

            List<FileNode> snapshotList = new List<FileNode>();

            foreach (var file in dirNode.FileNodes)
            {
                CloudBlob cloudBlob = CloudBlobHelper.GetCloudBlobReference(cloudBlobDir, file.Name, this.BlobType);
                this.GenerateFile(file, cloudBlob, snapshotList);
            }

            foreach (var snapshot in snapshotList)
            {
                dirNode.AddFileNode(snapshot);
            }
        }

        private void CheckFileNode(FileNode fileNode)
        {
            if (fileNode.LastModifiedTime != null)
            {
                throw new InvalidOperationException("Can't set LastModifiedTime to cloud blob");
            }

            if (fileNode.FileAttr != null)
            {
                throw new InvalidOperationException("Can't set file attribute to cloud blob");
            }
        }

        private void GenerateFile(FileNode fileNode, CloudBlob cloudBlob, List<FileNode> snapshotList)
        {
            this.CheckFileNode(fileNode);

            if ((StorageBlob.BlobType.PageBlob == cloudBlob.BlobType) && (fileNode.SizeInByte % 512 != 0))
            {
                throw new InvalidOperationException(string.Format("Can only generate page blob which size is multiple of 512bytes. Expected size is {0}", fileNode.SizeInByte));
            }

            string tempFileName = Guid.NewGuid().ToString();
            string localFilePath = Path.Combine(this.TempFolder, tempFileName);
            DMLibDataHelper.CreateLocalFile(fileNode, localFilePath);

            BlobRequestOptions storeMD5Options = new BlobRequestOptions()
            {
                RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(90), 3),
                StoreBlobContentMD5 = true,
            };

            cloudBlob.UploadFromFile(localFilePath, FileMode.Open, options: storeMD5Options);

            if (null != fileNode.MD5 ||
                null != fileNode.ContentType ||
                null != fileNode.CacheControl ||
                null != fileNode.ContentDisposition ||
                null != fileNode.ContentEncoding ||
                null != fileNode.ContentLanguage)
            {
                cloudBlob.Properties.ContentMD5 = fileNode.MD5;
                cloudBlob.Properties.ContentType = fileNode.ContentType;
                cloudBlob.Properties.CacheControl = fileNode.CacheControl;
                cloudBlob.Properties.ContentDisposition = fileNode.ContentDisposition;
                cloudBlob.Properties.ContentEncoding = fileNode.ContentEncoding;
                cloudBlob.Properties.ContentLanguage = fileNode.ContentLanguage;
                cloudBlob.SetProperties(options: HelperConst.DefaultBlobOptions);
            }

            if (null != fileNode.Metadata && fileNode.Metadata.Count > 0)
            {
                cloudBlob.Metadata.Clear();
                foreach (var metaData in fileNode.Metadata)
                {
                    cloudBlob.Metadata.Add(metaData);
                }

                cloudBlob.SetMetadata(options: HelperConst.DefaultBlobOptions);
            }

            cloudBlob.FetchAttributes(options: HelperConst.DefaultBlobOptions);
            this.BuildFileNode(cloudBlob, fileNode);

            for (int i = 0; i < fileNode.SnapshotsCount; ++i)
            {
                CloudBlob snapshot = cloudBlob.Snapshot();
                snapshotList.Add(this.BuildSnapshotFileNode(snapshot, fileNode.Name));
            }
        }

        private void BuildDirNode(CloudBlobDirectory cloudDir, DirNode dirNode)
        {
            foreach (IListBlobItem item in cloudDir.ListBlobs(false, BlobListingDetails.Metadata, HelperConst.DefaultBlobOptions))
            {
                CloudBlob cloudBlob = item as CloudBlob;
                CloudBlobDirectory subCloudDir = item as CloudBlobDirectory;

                if (cloudBlob != null)
                {
                    if (CloudBlobHelper.MapStorageBlobTypeToBlobType(cloudBlob.BlobType) == this.BlobType)
                    {
                        FileNode fileNode = new FileNode(cloudBlob.GetShortName());
                        this.BuildFileNode(cloudBlob, fileNode);
                        dirNode.AddFileNode(fileNode);
                    }
                }
                else if (subCloudDir != null)
                {
                    var subDirName = subCloudDir.GetShortName();
                    DirNode subDirNode = dirNode.GetDirNode(subDirName);

                    // A blob directory could be listed more than once if it's across table servers.
                    if (subDirNode == null)
                    {
                        subDirNode = new DirNode(subDirName);
                        this.BuildDirNode(subCloudDir, subDirNode);
                        dirNode.AddDirNode(subDirNode);
                    }
                }
            }
        }

        private FileNode BuildSnapshotFileNode(CloudBlob cloudBlob, string fileName)
        {
            FileNode fileNode = new FileNode(DMLibTestHelper.AppendSnapShotTimeToFileName(fileName, cloudBlob.SnapshotTime));
            this.BuildFileNode(cloudBlob, fileNode);
            return fileNode;
        }

        private void BuildFileNode(CloudBlob cloudBlob, FileNode fileNode)
        {
            fileNode.SizeInByte = cloudBlob.Properties.Length;
            fileNode.MD5 = cloudBlob.Properties.ContentMD5;
            fileNode.ContentType = cloudBlob.Properties.ContentType;
            fileNode.CacheControl = cloudBlob.Properties.CacheControl;
            fileNode.ContentDisposition = cloudBlob.Properties.ContentDisposition;
            fileNode.ContentEncoding = cloudBlob.Properties.ContentEncoding;
            fileNode.ContentLanguage = cloudBlob.Properties.ContentLanguage;
            fileNode.Metadata = cloudBlob.Metadata;

            DateTimeOffset dateTimeOffset = (DateTimeOffset)cloudBlob.Properties.LastModified;
            fileNode.LastModifiedTime = dateTimeOffset.UtcDateTime;
        }
    }
}
