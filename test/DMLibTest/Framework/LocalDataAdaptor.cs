//------------------------------------------------------------------------------
// <copyright file="LocalDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.WindowsAzure.Storage.Auth;
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

        protected override void GenerateDataImp(DMLibDataInfo dataInfo)
        {
            this.GenerateDir(dataInfo.RootNode, Path.Combine(this.BasePath, dataInfo.RootPath));
        }

        public override void ValidateMD5ByDownloading(object file)
        {
            throw new NotSupportedException();
        }

        public override DMLibDataInfo GetTransferDataInfo(string rootDir)
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
            this.BuildDirNode(rootDirInfo, dataInfo.RootNode);

            return dataInfo;
        }

        private void GenerateDir(DirNode dirNode, string parentPath)
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
                GenerateFile(file, dirPath);
            }

            foreach (var subDir in dirNode.NormalDirNodes)
            {
                GenerateDir(subDir, dirPath);
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

        private void GenerateFile(FileNode fileNode, string parentPath)
        {
            this.CheckFileNode(fileNode);

            string localFilePath = Path.Combine(parentPath, fileNode.Name);
            DMLibDataHelper.CreateLocalFile(fileNode, localFilePath);
#if DOTNET5_4
            FileInfo fileInfo = new FileInfo(localFilePath);

            this.BuildFileNode(fileInfo, fileNode);
#else
            this.BuildFileNode(localFilePath, fileNode);
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
                this.BuildFileNode(fileInfo, fileNode);
            }

            foreach (DirNode dirNode in parent.DirNodes)
            {
                BuildSymlinkedDirNode(dirNode, Path.Combine(parentPath, dirNode.Name));
            }
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

        private void BuildDirNode(string dirPath, DirNode parent)
        {
            dirPath = AppendDirectorySeparator(dirPath);
            foreach (var fileInfo in LongPathDirectoryExtension.GetFiles(dirPath))
            {
                FileNode fileNode = new FileNode(fileInfo.Remove(0,dirPath.Length));
                this.BuildFileNode(fileInfo, fileNode);
                parent.AddFileNode(fileNode);
            }

            foreach (var subDirInfo in LongPathDirectoryExtension.GetDirectories(dirPath))
            {
                DirNode subDirNode = new DirNode(subDirInfo.Remove(0, dirPath.Length));
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

        private void BuildFileNode(string path, FileNode fileNode)
        {
            fileNode.MD5 = Helper.GetFileContentMD5(LongPathExtension.GetFullPath(path));
            // fileNode.LastModifiedTime =
            using (FileStream fs = LongPathFileExtension.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileNode.SizeInByte = fs.Length;
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
