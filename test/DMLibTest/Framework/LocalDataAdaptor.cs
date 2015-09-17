//------------------------------------------------------------------------------
// <copyright file="LocalDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    internal class LocalDataAdaptor : LocalDataAdaptorBase<DMLibDataInfo>
    {
        private bool useStream;

        public LocalDataAdaptor(string basePath, SourceOrDest sourceOrDest, bool useStream = false)
            : base(basePath, sourceOrDest)
        {
            this.useStream = useStream;
        }

        public override object GetTransferObject(FileNode fileNode)
        {
            string filePath = Path.Combine(this.BasePath, fileNode.GetLocalRelativePath());

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

        public override object GetTransferObject(DirNode dirNode)
        {
            if (this.useStream)
            {
                throw new InvalidOperationException("Can't get directory transfer object in stream data adaptor.");
            }

            return Path.Combine(this.BasePath, dirNode.GetLocalRelativePath());
        }

        protected override void GenerateDataImp(DMLibDataInfo dataInfo)
        {
            this.GenerateDir(dataInfo.RootNode, Path.Combine(this.BasePath, dataInfo.RootPath));
        }

        public override DMLibDataInfo GetTransferDataInfo(string rootDir)
        {
            DirectoryInfo rootDirInfo = new DirectoryInfo(Path.Combine(this.BasePath, rootDir));
            if (!rootDirInfo.Exists)
            {
                return null;
            }

            DMLibDataInfo dataInfo = new DMLibDataInfo(rootDir);
            this.BuildDirNode(rootDirInfo, dataInfo.RootNode);

            return dataInfo;
        }

        private void GenerateDir(DirNode dirNode, string parentPath)
        {
            string dirPath = Path.Combine(parentPath, dirNode.Name);
            DMLibDataHelper.CreateLocalDirIfNotExists(dirPath);

            foreach (var subDir in dirNode.DirNodes)
            {
                GenerateDir(subDir, dirPath);
            }

            foreach (var file in dirNode.FileNodes)
            {
                GenerateFile(file, dirPath);
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

        private void GenerateFile(FileNode fileNode, string parentPath)
        {
            this.CheckFileNode(fileNode);

            string localFilePath = Path.Combine(parentPath, fileNode.Name);
            DMLibDataHelper.CreateLocalFile(fileNode, localFilePath);

            FileInfo fileInfo = new FileInfo(localFilePath);

            this.BuildFileNode(fileInfo, fileNode);
        }

        private void BuildDirNode(DirectoryInfo dirInfo, DirNode parent)
        {
            foreach (FileInfo fileInfo in dirInfo.GetFiles())
            {
                FileNode fileNode = new FileNode(fileInfo.Name);
                this.BuildFileNode(fileInfo, fileNode);
                parent.AddFileNode(fileNode);
            }

            foreach (DirectoryInfo subDirInfo in dirInfo.GetDirectories())
            {
                DirNode subDirNode = new DirNode(subDirInfo.Name);
                this.BuildDirNode(subDirInfo, subDirNode);
                parent.AddDirNode(subDirNode);
            }
        }

        private void BuildFileNode(FileInfo fileInfo, FileNode fileNode)
        {
            fileNode.MD5 = Helper.GetFileContentMD5(fileInfo.FullName);
            fileNode.LastModifiedTime = fileInfo.LastWriteTimeUtc;
            fileNode.SizeInByte = fileInfo.Length;
            fileNode.Metadata = new Dictionary<string, string>();
        }
    }
}
