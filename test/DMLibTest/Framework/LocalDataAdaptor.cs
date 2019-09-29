//------------------------------------------------------------------------------
// <copyright file="LocalDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.Azure.Storage.Auth;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using DMLibTest.Framework;
    using MS.Test.Common.MsTestLib;
    using System.Threading;

    internal class LocalDataAdaptor : LocalDataAdaptorBase<DMLibDataInfo>
    {
        private bool useStream;

        public LocalDataAdaptor(string basePath, SourceOrDest sourceOrDest, bool useStream = false)
            : base(basePath, sourceOrDest)
        {
            this.useStream = useStream;
        }

        public override object GetTransferObject(string rootPath, FileNode fileNode, StorageCredentials credentials = null)
        {
            if (credentials != null)
            {
                throw new NotSupportedException("Credentials is not supported in LocalDataAdaptor.");
            }

            string filePath = Path.Combine(this.BasePath, rootPath, fileNode.GetLocalRelativePath());

            if (this.useStream)
            {
                if (SourceOrDest.Source == this.SourceOrDest)
                {
                    return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                }
                else
                {
                    return new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                }
            }
            else
            {
                return filePath;
            }
        }

        public object GetTransferObject(string rootPath, string relativePath, StorageCredentials credentials = null)
        {
            if (credentials != null)
            {
                throw new NotSupportedException("Credentials is not supported in LocalDataAdaptor.");
            }

            string filePath = Path.Combine(this.BasePath, rootPath, relativePath);

            if (this.useStream)
            {
                if (SourceOrDest.Source == this.SourceOrDest)
                {
                    return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                }
                else
                {
                    return new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                }
            }
            else
            {
                return filePath;
            }
        }

        public override object GetTransferObject(string rootPath, DirNode dirNode, StorageCredentials credentials = null)
        {
            if (this.useStream)
            {
                throw new InvalidOperationException("Can't get directory transfer object in stream data adaptor.");
            }

            if (credentials != null)
            {
                throw new NotSupportedException("Credentials is not supported in LocalDataAdaptor.");
            }

            return Path.Combine(this.BasePath, rootPath, dirNode.GetLocalRelativePath());
        }

        public void GenerateDataInfo(DMLibDataInfo dataInfo, bool handleSMBAttributes)
        {
            this.GenerateDir(dataInfo.RootNode, Path.Combine(this.BasePath, dataInfo.RootPath), handleSMBAttributes);
        }

        protected override void GenerateDataImp(DMLibDataInfo dataInfo)
        {
            this.GenerateDir(dataInfo.RootNode, Path.Combine(this.BasePath, dataInfo.RootPath));
        }

        public override void ValidateMD5ByDownloading(object file)
        {
            throw new NotSupportedException();
        }

        public DMLibDataInfo GetTransferDataInfo(string rootDir, bool handleSMBAttributes)
        {
#if DOTNET5_4
            DirectoryInfo rootDirInfo = new DirectoryInfo(Path.Combine(this.BasePath, rootDir));
            if (!rootDirInfo.Exists)
            {
                return null;
            }
#else
            string rootDirInfo = rootDir;
            if(rootDir.Length == 0)
            {
                rootDirInfo = LongPathExtension.Combine(this.BasePath, rootDir);
            }
            if(!LongPathDirectoryExtension.Exists(rootDirInfo))
            {
                return null;
            }
#endif
            DMLibDataInfo dataInfo = new DMLibDataInfo(rootDir);
            this.BuildDirNode(rootDirInfo, dataInfo.RootNode, true);

            return dataInfo;
        }

        public override DMLibDataInfo GetTransferDataInfo(string rootDir)
        {
            return this.GetTransferDataInfo(rootDir, false);
        }

        private void GenerateDir(DirNode dirNode, string parentPath, bool handleSMBAttributes = false)
        {
            string dirPath = Path.Combine(parentPath, dirNode.Name);

            if (!string.IsNullOrEmpty(dirNode.Content))
            {
                CreateSymlink(dirPath, dirNode.Content);

                Test.Info("Building symlinked dir info of {0}", dirPath);
                dirNode.BuildSymlinkedDirNode();
                return;
            }
            
            DMLibDataHelper.CreateLocalDirIfNotExists(dirPath);

            foreach (var file in dirNode.FileNodes)
            {
                GenerateFile(file, dirPath, handleSMBAttributes);
            }

            foreach (var subDir in dirNode.NormalDirNodes)
            {
                GenerateDir(subDir, dirPath, handleSMBAttributes);
            }

            foreach (var subDir in dirNode.SymlinkedDirNodes)
            {
                CreateSymlink(Path.Combine(dirPath, subDir.Name), subDir.Content);

                Test.Info("Building symlinked dir info of {0}", Path.Combine(dirPath, subDir.Name));
                subDir.BuildSymlinkedDirNode();
            }
        }
        
        private static void CreateSymlink(string path, string target)
        {
            if (CrossPlatformHelpers.IsLinux)
            {
                TestHelper.RunCmd("ln", $@"-s {target} {path}");
            }
            else
            {
                //throw new PlatformNotSupportedException();
            }
        }

        private void CheckFileNode(FileNode fileNode)
        {
            if (fileNode.MD5 != null)
            {
                throw new InvalidOperationException("Can't set MD5 to local file");
            }

            if (fileNode.ContentType != null)
            {
                throw new InvalidOperationException("Can't set ContentType to local file");
            }

            if (fileNode.CacheControl != null)
            {
                throw new InvalidOperationException("Can't set CacheControl to local file");
            }

            if (fileNode.ContentDisposition != null)
            {
                throw new InvalidOperationException("Can't set ContentDisposition to local file");
            }

            if (fileNode.ContentEncoding != null)
            {
                throw new InvalidOperationException("Can't set ContentEncoding to local file");
            }

            if (fileNode.ContentLanguage != null)
            {
                throw new InvalidOperationException("Can't set ContentLanguage to local file");
            }

            if (fileNode.Metadata != null && fileNode.Metadata.Count > 0)
            {
                throw new InvalidOperationException("Can't set Metadata to local file");
            }
        }

        private void GenerateFile(FileNode fileNode, string parentPath, bool handleSMBAttributes = false)
        {
            this.CheckFileNode(fileNode);

            string localFilePath = Path.Combine(parentPath, fileNode.Name);
            DMLibDataHelper.CreateLocalFile(fileNode, localFilePath);

            if (handleSMBAttributes)
            {
                LongPathFileExtension.SetAttributes(localFilePath, Helper.ToFileAttributes(fileNode.SMBAttributes.Value));
            }

#if DOTNET5_4
            FileInfo fileInfo = new FileInfo(localFilePath);

            this.BuildFileNode(fileInfo, fileNode, handleSMBAttributes);
#else
            this.BuildFileNode(localFilePath, fileNode, handleSMBAttributes);
#endif
        }

        private void BuildSymlinkedDirNode(DirNode parent, string parentPath)
        {
            if (!CrossPlatformHelpers.IsLinux)
            {
                throw new PlatformNotSupportedException();
            }

            foreach (FileNode fileNode in parent.FileNodes)
            {
                Test.Info("Building file info of {0}", Path.Combine(parentPath, fileNode.Name));
                FileInfo fileInfo = new FileInfo(Path.Combine(parentPath, fileNode.Name));
                this.BuildFileNode(fileInfo, fileNode, false);
            }

            foreach (DirNode dirNode in parent.DirNodes)
            {
                BuildSymlinkedDirNode(dirNode, Path.Combine(parentPath, dirNode.Name));
            }
        }

        private void BuildDirNode(DirectoryInfo dirInfo, DirNode parent, bool handleSMBAttributes = false)
        {
            if (handleSMBAttributes)
            {
                DateTimeOffset? creationTime = null;
                DateTimeOffset? lastWriteTime = null;
                FileAttributes? fileAttributes = null;

#if DOTNET5_4
                LongPathFileExtension.GetFileProperties(dirInfo.FullName, out creationTime, out lastWriteTime, out fileAttributes, true);
#else
                LongPathFileExtension.GetFileProperties(dirInfo.FullName, out creationTime, out lastWriteTime, out fileAttributes);
#endif
                parent.CreationTime = creationTime;
                parent.LastWriteTime = lastWriteTime;
            }

            foreach (FileInfo fileInfo in dirInfo.GetFiles())
            {
                FileNode fileNode = new FileNode(fileInfo.Name);
                this.BuildFileNode(fileInfo, fileNode, handleSMBAttributes);
                parent.AddFileNode(fileNode);
            }

            foreach (DirectoryInfo subDirInfo in dirInfo.GetDirectories())
            {
                DirNode subDirNode = new DirNode(subDirInfo.Name);
                this.BuildDirNode(subDirInfo, subDirNode, handleSMBAttributes);
                parent.AddDirNode(subDirNode);
            }
        }

        private void BuildDirNode(string dirPath, DirNode parent, bool handleSMBAttributes)
        {
            dirPath = AppendDirectorySeparator(dirPath);

            DateTimeOffset? creationTime = null;
            DateTimeOffset? lastWriteTime = null;
            FileAttributes? fileAttributes = null;

#if DOTNET5_4
            LongPathFileExtension.GetFileProperties(dirPath, out creationTime, out lastWriteTime, out fileAttributes, true);
#else
            LongPathFileExtension.GetFileProperties(dirPath, out creationTime, out lastWriteTime, out fileAttributes);
#endif

            parent.CreationTime = creationTime;
            parent.LastWriteTime = lastWriteTime;

            foreach (var fileInfo in LongPathDirectoryExtension.GetFiles(dirPath))
            {
                FileNode fileNode = new FileNode(fileInfo.Remove(0,dirPath.Length));
                this.BuildFileNode(fileInfo, fileNode, handleSMBAttributes);
                parent.AddFileNode(fileNode);
            }

            foreach (var subDirInfo in LongPathDirectoryExtension.GetDirectories(dirPath))
            {
                DirNode subDirNode = new DirNode(subDirInfo.Remove(0, dirPath.Length));
                this.BuildDirNode(subDirInfo, subDirNode, handleSMBAttributes);
                parent.AddDirNode(subDirNode);
            }
        }

        private void BuildFileNode(FileInfo fileInfo, FileNode fileNode, bool handleSMBAttributes)
        {
            fileNode.MD5 = Helper.GetFileContentMD5(fileInfo.FullName);
            fileNode.LastModifiedTime = fileInfo.LastWriteTimeUtc;
            fileNode.SizeInByte = fileInfo.Length;
            fileNode.Metadata = new Dictionary<string, string>();

            if (CrossPlatformHelpers.IsWindows)
            {
                DateTimeOffset? creationTime = null;
                DateTimeOffset? lastWriteTime = null;
                FileAttributes? fileAttributes = null;
                LongPathFileExtension.GetFileProperties(fileInfo.FullName, out creationTime, out lastWriteTime, out fileAttributes);

                fileNode.CreationTime = creationTime;
                fileNode.LastWriteTime = lastWriteTime;

                if (handleSMBAttributes)
                {
                    fileNode.SMBAttributes = Helper.ToCloudFileNtfsAttributes(fileAttributes.Value);
                }
            }
        }

        private void BuildFileNode(string path, FileNode fileNode, bool handleSMBAttributes)
        {
            fileNode.MD5 = Helper.GetFileContentMD5(LongPathExtension.GetFullPath(path));
            // fileNode.LastModifiedTime =
            using (FileStream fs = LongPathFileExtension.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileNode.SizeInByte = fs.Length;
            }

            DateTimeOffset? creationTime = null;
            DateTimeOffset? lastWriteTime = null;
            FileAttributes? fileAttributes = null;
            LongPathFileExtension.GetFileProperties(path, out creationTime, out lastWriteTime, out fileAttributes);

            fileNode.CreationTime = creationTime;
            fileNode.LastWriteTime = lastWriteTime;
            if (handleSMBAttributes)
            {
                fileNode.SMBAttributes = Helper.ToCloudFileNtfsAttributes(fileAttributes.Value);
            }

            fileNode.Metadata = new Dictionary<string, string>();
        }

        private static string AppendDirectorySeparator(string dir)
        {
            char lastC = dir[dir.Length - 1];
            if (Path.DirectorySeparatorChar != lastC && Path.AltDirectorySeparatorChar != lastC)
            {
                dir = dir + Path.DirectorySeparatorChar;
            }

            return dir;
        }
    }
}
