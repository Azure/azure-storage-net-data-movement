//------------------------------------------------------------------------------
// <copyright file="CloudFileDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.RetryPolicies;
    using MS.Test.Common.MsTestLib;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Text.RegularExpressions;
    internal class CloudFileDataAdaptor : DataAdaptor<DMLibDataInfo>
    {
        private TestAccount testAccount;
        public CloudFileHelper fileHelper;
        private string tempFolder;
        private readonly string defaultShareName;
        private string shareName;
        private DateTimeOffset? snapshotTime;

        public override string StorageKey
        {
            get
            {
                return this.fileHelper.Account.Credentials.ExportBase64EncodedKey();
            }
        }

        public CloudFileDataAdaptor(TestAccount testAccount, string shareName, SourceOrDest sourceOrDest)
        {
            this.testAccount = testAccount;
            this.fileHelper = new CloudFileHelper(testAccount.Account);
            this.shareName = shareName;
            this.defaultShareName = shareName;
            this.tempFolder = Guid.NewGuid().ToString();
            this.SourceOrDest = sourceOrDest;
            this.snapshotTime = null;
        }

        public string ShareName
        {
            get
            {
                return this.shareName;
            }

            set
            {
                this.shareName = value;
            }
        }

        public CloudFileHelper FileHelper
        {
            get
            {
                return this.fileHelper;
            }

            private set
            {
                this.fileHelper = value;
            }
        }

        public CloudFileShare GetBaseShare()
        {
            return fileHelper.FileClient.GetShareReference(ShareName);
        }

        public override object GetTransferObject(string rootPath, FileNode fileNode, StorageCredentials credentials = null)
        {
            return this.GetCloudFileReference(rootPath, fileNode, credentials);
        }

        public override object GetTransferObject(string rootPath, DirNode dirNode, StorageCredentials credentials = null)
        {
            return this.GetCloudFileDirReference(rootPath, dirNode, credentials);
        }

        public override string GetAddress(params string[] list)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(this.testAccount.GetEndpointBaseUri(EndpointType.File) + "/" + this.shareName + "/");

            foreach (string token in list)
            {
                if (!string.IsNullOrEmpty(token))
                {
                    builder.Append(token);
                    builder.Append("/");
                }
            }

            return builder.ToString();
        }

        public override string GetSecondaryAddress(params string[] list)
        {
            throw new NotSupportedException("GetSecondaryAddress is not supported in CloudFileDataAdaptor.");
        }

        public override void CreateIfNotExists()
        {
            this.fileHelper.CreateShare(this.shareName);
            snapshotTime = null;
        }

        public override bool Exists()
        {
            return this.fileHelper.Exists(this.shareName);
        }

        public override void WaitForGEO()
        {
            throw new NotSupportedException("WaitForGEO is not supported in CloudFileDataAdaptor.");
        }

        public override void ValidateMD5ByDownloading(object file)
        {
            ((CloudFile)file).DownloadToStream(Stream.Null, null, new FileRequestOptions()
            {
                RetryPolicy = DMLibTestConstants.DefaultRetryPolicy,
                DisableContentMD5Validation = false,
                UseTransactionalMD5 = true,
                MaximumExecutionTime = DMLibTestConstants.DefaultExecutionTimeOut,
            });
        }

        public string MountFileShare()
        {
            this.fileHelper.CreateShare(this.shareName);
            CloudFileShare share = this.fileHelper.FileClient.GetShareReference(this.shareName);

            string cmd = "net";
            string args = string.Format(
                "use * {0} {1} /USER:{2}",
                string.Format(@"\\{0}\{1}", share.Uri.Host, this.shareName),
                this.fileHelper.Account.Credentials.ExportBase64EncodedKey(),
                this.fileHelper.Account.Credentials.AccountName);

            string stdout, stderr;
            int ret = TestHelper.RunCmd(cmd, args, out stdout, out stderr);
            Test.Assert(0 == ret, "mounted to xsmb share successfully");
            Test.Info("stdout={0}, stderr={1}", stdout, stderr);

            Regex r = new Regex(@"Drive (\S+) is now connected to");
            Match m = r.Match(stdout);
            if (m.Success)
            {
                return m.Groups[1].Value;
            }
            else
            {
                return null;
            }
        }

        public void UnmountFileShare(string deviceName)
        {
            string cmd = "net";
            string args = string.Format("use {0} /DELETE", deviceName);
            string stdout, stderr;
            int ret = TestHelper.RunCmd(cmd, args, out stdout, out stderr);
            Test.Assert(0 == ret, "unmounted {0} successfully", deviceName);
            Test.Info("stdout={0}, stderr={1}", stdout, stderr);
        }

        public CloudFile GetCloudFileReference(string rootPath, FileNode fileNode, StorageCredentials credentials = null)
        {
            var share = this.fileHelper.FileClient.GetShareReference(this.shareName, snapshotTime);

            if (credentials != null)
            {
                share = new CloudFileShare(share.SnapshotQualifiedStorageUri, credentials);
            }

            string fileName = fileNode.GetURLRelativePath();
            if (fileName.StartsWith("/"))
            {
                fileName = fileName.Substring(1, fileName.Length - 1);
            }

            if (!string.IsNullOrEmpty(rootPath))
            {
                fileName = rootPath + "/" + fileName;
            }
            
            return share.GetRootDirectoryReference().GetFileReference(fileName);
        }
        
        public CloudFileDirectory GetCloudFileDirReference(string rootPath, DirNode dirNode, StorageCredentials credentials = null)
        {
            var share = this.fileHelper.FileClient.GetShareReference(this.shareName, snapshotTime);

            if (credentials != null)
            {
                share = new CloudFileShare(share.SnapshotQualifiedStorageUri, credentials);
            }
            string dirName = dirNode.GetURLRelativePath();
            if (dirName.StartsWith("/"))
            {
                dirName = dirName.Substring(1, dirName.Length - 1);
            }

            if (!string.IsNullOrEmpty(rootPath))
            {
                dirName = rootPath + "/" + dirName;
            }

            if (string.IsNullOrEmpty(dirName))
            {
                return share.GetRootDirectoryReference();
            }
            else
            {
                return share.GetRootDirectoryReference().GetDirectoryReference(dirName);
            }
        }

        protected override void GenerateDataImp(DMLibDataInfo dataInfo)
        {
            fileHelper.CreateShare(this.shareName);

            using (TemporaryTestFolder localTemp = new TemporaryTestFolder(this.tempFolder))
            {
                CloudFileDirectory rootCloudFileDir = this.fileHelper.GetDirReference(this.shareName, dataInfo.RootPath);
                this.GenerateDir(dataInfo.RootNode, rootCloudFileDir, this.tempFolder);

                if (dataInfo.IsFileShareSnapshot)
                {
                    CloudFileShare baseShare = this.fileHelper.FileClient.GetShareReference(this.shareName);
                    this.snapshotTime = baseShare.SnapshotAsync().Result.SnapshotTime;
                    CloudFileHelper.CleanupFileDirectory(baseShare.GetRootDirectoryReference());
                }
            }
        }

        private void GenerateDir(DirNode dirNode, CloudFileDirectory cloudFileDir, string parentPath)
        {
            string dirPath = Path.Combine(parentPath, dirNode.Name);
            DMLibDataHelper.CreateLocalDirIfNotExists(dirPath);
            cloudFileDir.CreateIfNotExists(HelperConst.DefaultFileOptions);
            
            if (null != cloudFileDir.Parent)
            {
                if (null != dirNode.SMBAttributes)
                {
                    cloudFileDir.Properties.NtfsAttributes = dirNode.SMBAttributes;
                }

                if (dirNode.CreationTime.HasValue) cloudFileDir.Properties.CreationTime = dirNode.CreationTime;
                if (dirNode.LastWriteTime.HasValue) cloudFileDir.Properties.LastWriteTime = dirNode.LastWriteTime;

                cloudFileDir.SetProperties(HelperConst.DefaultFileOptions);
                cloudFileDir.FetchAttributes(null, HelperConst.DefaultFileOptions);

                dirNode.CreationTime = cloudFileDir.Properties.CreationTime;
                dirNode.LastWriteTime = cloudFileDir.Properties.LastWriteTime;

                if ((null != dirNode.Metadata)
                    && (dirNode.Metadata.Count > 0))
                {
                    cloudFileDir.Metadata.Clear();

                    foreach (var keyValuePair in dirNode.Metadata)
                    {
                        cloudFileDir.Metadata.Add(keyValuePair);
                    }

                    cloudFileDir.SetMetadata(null, HelperConst.DefaultFileOptions);
                }
            }

            foreach (var subDir in dirNode.DirNodes)
            {
                CloudFileDirectory subCloudFileDir = cloudFileDir.GetDirectoryReference(subDir.Name);
                this.GenerateDir(subDir, subCloudFileDir, dirPath);
            }

            foreach (var file in dirNode.FileNodes)
            {
                CloudFile cloudFile = cloudFileDir.GetFileReference(file.Name);
                this.GenerateFile(file, cloudFile, dirPath);
            }
        }

        private void CheckFileNode(FileNode fileNode)
        {
            if (fileNode.LastModifiedTime != null)
            {
                throw new InvalidOperationException("Can't set LastModifiedTime to cloud file");
            }

            if (fileNode.FileAttr != null)
            {
                throw new InvalidOperationException("Can't set file attribute to cloud file");
            }
        }

        private void GenerateFile(FileNode fileNode, CloudFile cloudFile, string parentPath)
        {
            this.CheckFileNode(fileNode);

            string tempFileName = Guid.NewGuid().ToString();
            string localFilePath = Path.Combine(parentPath, tempFileName);
            DMLibDataHelper.CreateLocalFile(fileNode, localFilePath);

            FileRequestOptions storeMD5Options = new FileRequestOptions()
            {
                RetryPolicy = DMLibTestConstants.DefaultRetryPolicy,
                StoreFileContentMD5 = true,
                MaximumExecutionTime = DMLibTestConstants.DefaultExecutionTimeOut
            };

            cloudFile.UploadFromFile(localFilePath, options: storeMD5Options);

            if (null != fileNode.MD5 ||
                null != fileNode.ContentType ||
                null != fileNode.CacheControl ||
                null != fileNode.ContentDisposition ||
                null != fileNode.ContentEncoding ||
                null != fileNode.ContentLanguage)
            {
                // set user defined MD5 to cloud file
                cloudFile.Properties.ContentMD5 = fileNode.MD5;
                cloudFile.Properties.ContentType = fileNode.ContentType;
                cloudFile.Properties.CacheControl = fileNode.CacheControl;
                cloudFile.Properties.ContentDisposition = fileNode.ContentDisposition;
                cloudFile.Properties.ContentEncoding = fileNode.ContentEncoding;
                cloudFile.Properties.ContentLanguage = fileNode.ContentLanguage;
                
                cloudFile.SetProperties(options: HelperConst.DefaultFileOptions);
            }

            if (null != fileNode.SMBAttributes)
            {
                cloudFile.Properties.NtfsAttributes = fileNode.SMBAttributes;
                cloudFile.Properties.CreationTime = fileNode.CreationTime;
                cloudFile.Properties.LastWriteTime = fileNode.LastWriteTime;
                cloudFile.SetProperties(options: HelperConst.DefaultFileOptions);
            }

            if (null != fileNode.Metadata && fileNode.Metadata.Count > 0)
            {
                cloudFile.Metadata.Clear();
                foreach (var metaData in fileNode.Metadata)
                {
                    cloudFile.Metadata.Add(metaData);
                }

                cloudFile.SetMetadata(options: HelperConst.DefaultFileOptions);
            }

            this.BuildFileNode(cloudFile, fileNode);
        }

        public override DMLibDataInfo GetTransferDataInfo(string rootDir)
        {
            CloudFileDirectory fileDir = fileHelper.QueryFileDirectory(this.shareName, rootDir);
            if (fileDir == null)
            {
                return null;
            }

            DMLibDataInfo dataInfo = new DMLibDataInfo(rootDir);

            this.BuildDirNode(fileDir, dataInfo.RootNode);
            return dataInfo;
        }

        public override void Cleanup()
        {
            this.fileHelper.CleanupShare(this.shareName);
            snapshotTime = null;
        }

        public override void DeleteLocation()
        {
            this.fileHelper.DeleteShare(this.shareName);
            snapshotTime = null;
        }

        public override void MakePublic()
        {
            throw new NotSupportedException("MakePublic is not supported in CloudFileDataAdaptor.");
        }

        public override void Reset()
        {
            this.shareName = defaultShareName;
        }

        public override string GenerateSAS(SharedAccessPermissions sap, int validatePeriod, string policySignedIdentifier = null)
        {
            if (null == policySignedIdentifier)
            {
                if (this.SourceOrDest == SourceOrDest.Dest)
                {
                    this.fileHelper.CreateShare(this.shareName);
                }

                return this.fileHelper.GetSASofShare(this.shareName, sap.ToFilePermissions(), validatePeriod, false);
            }
            else
            {
                this.fileHelper.CreateShare(this.shareName);
                return this.fileHelper.GetSASofShare(this.shareName, sap.ToFilePermissions(), validatePeriod, true, policySignedIdentifier);
            }
        }

        public override void RevokeSAS()
        {
            this.fileHelper.ClearSASPolicyofShare(this.shareName);
        }

        private void BuildDirNode(CloudFileDirectory cloudDir, DirNode dirNode)
        {
            cloudDir.FetchAttributes(options: HelperConst.DefaultFileOptions);
            dirNode.LastWriteTime = cloudDir.Properties.LastWriteTime;
            dirNode.CreationTime = cloudDir.Properties.CreationTime;
            dirNode.SMBAttributes = cloudDir.Properties.NtfsAttributes;

            if (cloudDir.Metadata.Count > 0)
            {
                dirNode.Metadata = new Dictionary<string, string>(cloudDir.Metadata);
            }

            foreach (IListFileItem item in cloudDir.ListFilesAndDirectories(HelperConst.DefaultFileOptions))
            {
                CloudFile cloudFile = item as CloudFile;
                CloudFileDirectory subCloudDir = item as CloudFileDirectory;

                if (cloudFile != null)
                {
                    // Cannot fetch attributes while listing, so do it for each cloud file.
                    cloudFile.FetchAttributes(options: HelperConst.DefaultFileOptions);

                    FileNode fileNode = new FileNode(cloudFile.Name);
                    this.BuildFileNode(cloudFile, fileNode);
                    dirNode.AddFileNode(fileNode);
                }
                else if (subCloudDir != null)
                {
                    DirNode subDirNode = new DirNode(subCloudDir.Name);
                    this.BuildDirNode(subCloudDir, subDirNode);
                    dirNode.AddDirNode(subDirNode);
                }
            }
        }

        private void BuildFileNode(CloudFile cloudFile, FileNode fileNode)
        {
            fileNode.SizeInByte = cloudFile.Properties.Length;
            fileNode.MD5 = cloudFile.Properties.ContentMD5;
            fileNode.ContentType = cloudFile.Properties.ContentType;
            fileNode.CacheControl = cloudFile.Properties.CacheControl;
            fileNode.ContentDisposition = cloudFile.Properties.ContentDisposition;
            fileNode.ContentEncoding = cloudFile.Properties.ContentEncoding;
            fileNode.ContentLanguage = cloudFile.Properties.ContentLanguage;
            fileNode.Metadata = cloudFile.Metadata;

            fileNode.LastWriteTime = cloudFile.Properties.LastWriteTime;
            fileNode.CreationTime = cloudFile.Properties.CreationTime;
            fileNode.SMBAttributes = cloudFile.Properties.NtfsAttributes;

            DateTimeOffset dateTimeOffset = (DateTimeOffset)cloudFile.Properties.LastModified;
            fileNode.LastModifiedTime = dateTimeOffset.UtcDateTime;
        }        
    }
}
