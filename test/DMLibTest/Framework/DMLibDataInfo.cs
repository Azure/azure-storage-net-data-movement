//------------------------------------------------------------------------------
// <copyright file="DMLibDataInfo.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Microsoft.Azure.Storage.File;

    public class DMLibDataInfo : IDataInfo
    {
        public DMLibDataInfo(string rootPath)
        {
            this.RootPath = rootPath;
            this.RootNode = new DirNode(string.Empty);
            IsFileShareSnapshot = false;
        }

        public int FileCount
        {
            get
            {
                return this.RootNode.FileNodeCountRecursive;
            }
        }

        public string RootPath
        {
            get;
            set;
        }

        public DirNode RootNode
        {
            get;
            set;
        }

        public IEnumerable<FileNode> EnumerateFileNodes()
        {
            return this.RootNode.EnumerateFileNodesRecursively();
        }

        IDataInfo IDataInfo.Clone()
        {
            return this.Clone();
        }

        public DMLibDataInfo Clone()
        {
            return new DMLibDataInfo(this.RootPath)
            {
                RootNode = this.RootNode.Clone(),
            };
        }

        public override string ToString()
        {
            return this.DetailedInfo();
        }

        public bool IsFileShareSnapshot
        {
            get;
            set;
        }
    }

    public class DataInfoNode
    {
        public string Name
        {
            get;
            set;
        }

        public DirNode Parent
        {
            get;
            set;
        }

        public IEnumerable<string> PathComponents
        {
            get
            {
                if (this.Parent != null)
                {
                    foreach (string component in Parent.PathComponents)
                    {
                        yield return component;
                    }
                    
                    yield return this.Name;
                }

            }
        }
    }

    public class FileNode : DataInfoNode, IComparable<FileNode>
    {
        public FileNode(string name)
        {
            this.Name = name;
        }

        public int SnapshotsCount
        {
            get;
            set;
        }

        public string MD5
        {
            get;
            set;
        }

        public string CacheControl
        {
            get;
            set;
        }

        public string ContentDisposition 
        {
            get;
            set;
        }

        public string ContentEncoding 
        {
            get;
            set;
        }
        
        public string ContentLanguage 
        {
            get;
            set;
        }

        public CloudFileNtfsAttributes? SMBAttributes
        {
            get;
            set;
        }

        public DateTimeOffset? CreationTime
        {
            get;
            set;
        }

        public DateTimeOffset? LastWriteTime
        {
            get;
            set;
        }

        public IDictionary<string, string> Metadata
        {
            get;
            set;
        }

        public string ContentType
        {
            get;
            set;
        }

        public DateTime? LastModifiedTime
        {
            get;
            set;
        }

        public long SizeInByte
        {
            get;
            set;
        }

        /// <summary>
        /// For block blob to specify the block size. Optional.
        /// </summary>
        public int? BlockSize { get; set; }

        public FileAttributes? FileAttr
        {
            get;
            set;
        }

        public string AbsolutePath
        {
            get;
            set;
        }

        public int CompareTo(FileNode other)
        {
            return string.Compare(this.Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

		public FileNode Clone(string name = null)
        {
            // Clone metadata
            Dictionary<string, string> cloneMetaData = null;
            if (this.Metadata != null)
            {
                cloneMetaData = new Dictionary<string, string>(this.Metadata);
            }

            return new FileNode(name ?? this.Name)
            {
                SnapshotsCount = this.SnapshotsCount,
                CacheControl = this.CacheControl,
                ContentDisposition = this.ContentDisposition,
                ContentEncoding = this.ContentEncoding,
                ContentLanguage = this.ContentLanguage,
                ContentType = this.ContentType,
                MD5 = this.MD5,
                Metadata = cloneMetaData,
                LastModifiedTime = this.LastModifiedTime,
                SizeInByte = this.SizeInByte,
                FileAttr = this.FileAttr,
                AbsolutePath = this.AbsolutePath,
                SMBAttributes = this.SMBAttributes,
                CreationTime = this.CreationTime,
                LastWriteTime = this.LastWriteTime
            };
        }
    }

    public class DirNode : DataInfoNode, IComparable<DirNode>
    {
        private Dictionary<string, DirNode> dirNodeMap;
        private Dictionary<string, FileNode> fileNodeMap;
        private string content = null;
        private DirNode symlinkTargetDirNode = null;

        public DirNode(string name)
        {
            this.Name = name;
            this.dirNodeMap = new Dictionary<string, DirNode>();
            this.fileNodeMap = new Dictionary<string, FileNode>();
        }

        public static DirNode SymlinkedDir(string name, string content, DirNode targetDir)
        {
            DirNode symlinkNode = targetDir.Clone();
            symlinkNode.content = content;
            symlinkNode.Name = name;
            symlinkNode.symlinkTargetDirNode = targetDir;
            return symlinkNode;
        }

        public int FileNodeCountRecursive
        {
            get
            {
                int totalCount = this.FileNodeCount;
                foreach (DirNode subDirNode in this.DirNodes)
                {
                    totalCount += subDirNode.FileNodeCountRecursive;
                }

                return totalCount;
            }
        }

        public string Content => this.content;

        public int FileNodeCount
        {
            get
            {
                return fileNodeMap.Count;
            }
        }

        public int DirNodeCount
        {
            get
            {
                return dirNodeMap.Count;
            }
        }

        public int NonEmptyDirNodeCount
        {
            get
            {
                int count = 0;
                foreach(DirNode subDirNode in dirNodeMap.Values)
                {
                    if (!subDirNode.IsEmpty)
                    {
                        count++;
                    }
                }

                return count;
            }
        }

        public bool IsEmpty
        {
            get
            {
                if (this.FileNodeCount != 0)
                {
                    return false;
                }

                foreach(DirNode subDirNode in dirNodeMap.Values)
                {
                    if (!subDirNode.IsEmpty)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        public IEnumerable<DirNode> NormalDirNodes
        {
            get
            {
                foreach (var dirNode in this.DirNodes)
                {
                    if (string.IsNullOrEmpty(dirNode.Content))
                    {
                        yield return dirNode;
                    }
                }
            }
        }

        public IEnumerable<DirNode> SymlinkedDirNodes
        {
            get
            {
                foreach (var dirNode in this.DirNodes)
                {
                    if (!string.IsNullOrEmpty(dirNode.Content))
                    {
                        yield return dirNode;
                    }
                }
            }
        }

        public CloudFileNtfsAttributes? SMBAttributes
        {
            get;
            set;
        }

        public DateTimeOffset? CreationTime
        {
            get;
            set;
        }

        public DateTimeOffset? LastWriteTime
        {
            get;
            set;
        }

        public IDictionary<string, string> Metadata
        {
            get;
            set;
        }

        public IEnumerable<DirNode> DirNodes
        {
            get
            {
                return dirNodeMap.Values;
            }
        }

        public IEnumerable<FileNode> FileNodes
        {
            get
            {
                return fileNodeMap.Values;
            }
        }

        public int CompareTo(DirNode other)
        {
            return string.Compare(this.Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public FileNode GetFileNode(string name)
        {
            FileNode result = null;
            if (this.fileNodeMap.TryGetValue(name, out result))
            {
                return result;
            }

            return null;
        }

        public DirNode GetDirNode(string name)
        {
            DirNode result = null;
            if (this.dirNodeMap.TryGetValue(name, out result))
            {
                return result;
            }

            return null;
        }

        public void AddDirNode(DirNode dirNode)
        {
            if (!string.IsNullOrEmpty(this.content))
            {
                throw new Exception("Symlinked dir, cannot add child to it. Please add child to its target");
            }

            dirNode.Parent = this;
            this.dirNodeMap.Add(dirNode.Name, dirNode);
        }

        public void AddFileNode(FileNode fileNode)
        {
            if (!string.IsNullOrEmpty(this.content))
            {
                throw new Exception("Symlinked dir, cannot add child to it. Please add child to its target");
            }

            fileNode.Parent = this;
            this.fileNodeMap.Add(fileNode.Name, fileNode);
        }

        public FileNode DeleteFileNode(string name)
        {
            if (!string.IsNullOrEmpty(this.content))
            {
                throw new Exception("Symlinked dir, cannot be modified. Please modify its target");
            }

            FileNode fn = null;
            if (this.fileNodeMap.ContainsKey(name))
            {
                fn = this.fileNodeMap[name];
                fn.Parent = null;
                this.fileNodeMap.Remove(name);
            }

            return fn;
        }

        public DirNode DeleteDirNode(string name)
        {
            if (!string.IsNullOrEmpty(this.content))
            {
                throw new Exception("Symlinked dir, cannot be modified. Please modify its target");
            }

            DirNode dn = null;
            if (this.dirNodeMap.ContainsKey(name))
            {
                dn = this.dirNodeMap[name];
                this.dirNodeMap.Remove(name);
            }

            return dn;
        }

        public DirNode Clone()
        {
            DirNode newDirNode = new DirNode(this.Name);
            newDirNode.SMBAttributes = this.SMBAttributes;
            newDirNode.CreationTime = this.CreationTime;
            newDirNode.LastWriteTime = this.LastWriteTime;

            if (null != this.Metadata)
            {
                newDirNode.Metadata = new Dictionary<string, string>(this.Metadata);
            }

            foreach(FileNode fileNode in this.FileNodes)
            {
                newDirNode.AddFileNode(fileNode.Clone());
            }

            foreach(DirNode dirNode in this.DirNodes)
            {
                newDirNode.AddDirNode(dirNode.Clone());
            }

            newDirNode.content = this.content;
            return newDirNode;
        }
        
        public IEnumerable<FileNode> EnumerateFileNodesRecursively()
        {
            foreach (var fileNode in this.FileNodes)
            {
                yield return fileNode;
            }

            foreach (DirNode subDirNode in this.DirNodes)
            {
                foreach (var fileNode in subDirNode.EnumerateFileNodesRecursively())
                {
                    yield return fileNode;
                }
            }
        }

        public IEnumerable<DirNode> EnumerateDirNodesRecursively()
        {
            foreach (DirNode subDirNode in this.DirNodes)
            {
                foreach (var dirNode in subDirNode.EnumerateDirNodesRecursively())
                {
                    yield return dirNode;
                }

                yield return subDirNode;
            }
        }

        public void BuildSymlinkedDirNode()
        {
            if (string.IsNullOrEmpty(this.content) || null == this.symlinkTargetDirNode)
            {
                throw new Exception("It's not a symlinked DirNode");
            }

            BuildSymlinkedDirNode(this, this.symlinkTargetDirNode);
        }

        private void BuildSymlinkedDirNode(DirNode symlinkSubDir, DirNode targetSubDir)
        {
            foreach (var fileNode in symlinkSubDir.FileNodes)
            {
                var targetFileNode = targetSubDir.GetFileNode(fileNode.Name);
                fileNode.MD5 = targetFileNode.MD5;
                fileNode.LastModifiedTime = targetFileNode.LastModifiedTime;
                fileNode.SizeInByte = targetFileNode.SizeInByte;
                fileNode.Metadata = new Dictionary<string, string>();
            }

            foreach (var subDir in symlinkSubDir.DirNodes)
            {
                BuildSymlinkedDirNode(subDir, targetSubDir.GetDirNode(subDir.Name));
            }
        }

        /// <summary>
        /// for debug use, show DataInfo in tree format
        /// </summary>
        public void Display(int level)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < level; ++i)
                sb.Append("--");
            sb.Append(this.Name);
            Console.WriteLine(sb.ToString());

            foreach (FileNode fn in fileNodeMap.Values)
            {
                StringBuilder fileNode = new StringBuilder();
                for (int i = 0; i < level + 1; ++i)
                {
                    fileNode.Append("--");
                }
                fileNode.Append(fn.Name);
                Console.WriteLine(fileNode.ToString());
            }

            foreach (DirNode dn in dirNodeMap.Values)
            {
                dn.Display(level + 1);
            }
        }
    }
}
