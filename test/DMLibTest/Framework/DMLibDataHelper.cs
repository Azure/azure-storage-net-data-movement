//------------------------------------------------------------------------------
// <copyright file="DMLibDataHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using Framework;

    internal static class DMLibDataHelper
    {
        public static void AddOneFile(DirNode dirNode, string fileName, long fileSizeInKB, FileAttributes? fa = null, DateTime? lmt = null, int? blockSize = null)
        {
            AddOneFileInBytes(dirNode, fileName, 1024L * fileSizeInKB, fa, lmt, blockSize);
        }

        public static void AddOneFileInBytes(DirNode dirNode, string fileName, long fileSizeInB, FileAttributes? fa = null, DateTime? lmt = null, int? blockSize = null)
        {
            FileNode fileNode = new FileNode(fileName)
            {
                SizeInByte = fileSizeInB,
                FileAttr = fa,
                LastModifiedTime = lmt,
                BlockSize = blockSize
            };

            dirNode.AddFileNode(fileNode);
        }

        public static FileNode RemoveOneFile(DirNode dirNode, string fileName)
        {
            return dirNode.DeleteFileNode(fileName);
        }

        public static DirNode RemoveOneDir(DirNode parentNode, string dirNodeToDelete)
        {
            return parentNode.DeleteDirNode(dirNodeToDelete);
        }

        public static void AddMultipleFiles(
            DirNode dirNode, 
            string filePrefix, 
            int fileNumber, 
            int fileSizeInKB, 
            FileAttributes? fa = null, 
            DateTime? lmt = null,
            string cacheControl = null,
            string contentDisposition = null,
            string contentEncoding = null,
            string contentLanguage = null,
            string contentType = null,
            string md5 = null,
            IDictionary<string, string> metadata = null)
        {
            DMLibDataHelper.AddTree(
                dirNode, 
                string.Empty, 
                filePrefix, 
                fileNumber, 
                0, 
                fileSizeInKB, 
                fa, 
                lmt,
                cacheControl,
                contentDisposition,
                contentEncoding,
                contentLanguage,
                contentType,
                md5,
                metadata);
        }

        public static void AddMultipleFilesNormalSize(DirNode dirNode, string filePrefix)
        {
            int[] fileSizes = new int[] { 0, 1, 4000, 4 * 1024, 10000 };
            AddMultipleFilesDifferentSize(dirNode, filePrefix, fileSizes);
        }

        public static void AddMultipleFilesBigSize(DirNode dirNode, string filePrefix)
        {
            int[] fileSizes = new int[] { 32000, 64 * 1024 };
            AddMultipleFilesDifferentSize(dirNode, filePrefix, fileSizes);
        }

        public static void AddMultipleFilesDifferentSize(DirNode dirNode, string filePrefix, int[] fileSizes)
        {
            for (int i = 0; i < fileSizes.Length; ++i)
            {
                FileNode fileNode = new FileNode(filePrefix + "_" + i)
                {
                    SizeInByte = fileSizes[i] * 1024
                };

                dirNode.AddFileNode(fileNode);
            }
        }

        public static void AddMultipleFilesTotalSize(DirNode dirNode, string filePrefix, int fileNumber, int totalSizeInKB, DateTime? lmt = null)
        {
            int fileSizeInKB = totalSizeInKB / fileNumber;
            fileSizeInKB = fileSizeInKB == 0 ? 1 : fileSizeInKB;
            DMLibDataHelper.AddMultipleFiles(dirNode, filePrefix, fileNumber, fileSizeInKB, lmt: lmt);
        }

        public static void AddTree(
            DirNode dirNode, 
            string dirPrefix, 
            string filePrefix, 
            int width, 
            int depth, 
            int fileSizeInKB, 
            FileAttributes? fa = null, 
            DateTime? lmt = null,
            string cacheControl = null,
            string contentDisposition = null,
            string contentEncoding = null,
            string contentLanguage = null,
            string contentType = null,
            string md5 = null,
            IDictionary<string, string> metadata = null)
        {
            for (int i = 0; i < width; ++i)
            {
                string fileName = i == 0 ? filePrefix : filePrefix + "_" + i;
                FileNode fileNode = new FileNode(fileName)
                {
                    SizeInByte = 1024L * fileSizeInKB,
                    FileAttr = fa,
                    LastModifiedTime = lmt,
                    CacheControl = cacheControl,
                    ContentDisposition = contentDisposition,
                    ContentEncoding = contentEncoding,
                    ContentLanguage = contentLanguage,
                    ContentType = contentType,
                    MD5 = md5,
                    Metadata = metadata
                };

                dirNode.AddFileNode(fileNode);
            }

            if (depth > 0)
            {
                for (int i = 0; i < width; ++i)
                {
                    string dirName = i == 0 ? dirPrefix : dirPrefix + "_" + i;
                    DirNode subDirNode = dirNode.GetDirNode(dirName);
                    if (subDirNode == null)
                    {
                        subDirNode = new DirNode(dirName);
                        dirNode.AddDirNode(subDirNode);
                    }

                    DMLibDataHelper.AddTree(subDirNode, dirPrefix, filePrefix, width, depth - 1, fileSizeInKB, fa, lmt: lmt);
                }
            }
        }

        public static void AddTreeTotalSize(DirNode dirNode, string dirPrefix, string filePrefix, int width, int depth, int totalSizeInKB, DateTime? lmt = null)
        {
            int fileNumber;
            if (width <= 1)
            {
                fileNumber = (depth + 1) * width;
            }
            else
            {
                int widthPowDepth = width;
                for (int i = 0; i < depth; ++i)
                {
                    widthPowDepth *= width;
                }

                fileNumber = width * (widthPowDepth - 1) / (width - 1);
            }

            int fileSizeInKB = totalSizeInKB / fileNumber;
            fileSizeInKB = fileSizeInKB == 0 ? 1 : fileSizeInKB;

            DMLibDataHelper.AddTree(dirNode, dirPrefix, filePrefix, width, depth, fileSizeInKB, lmt: lmt);
        }

        public static void CreateLocalDirIfNotExists(string dirPath)
        {
            if (!String.Equals(string.Empty, dirPath) && !LongPathDirectoryExtension.Exists(dirPath))
            {
                LongPathDirectoryExtension.CreateDirectory(dirPath);
            }
        }

        public static void CreateLocalFile(FileNode fileNode, string filePath)
        {
            Helper.GenerateFileInBytes(filePath, fileNode.SizeInByte);
            fileNode.AbsolutePath = filePath;

            if (fileNode.LastModifiedTime != null)
            {
                // Set last modified time
                FileInfo fileInfo = new FileInfo(filePath);
                fileInfo.LastWriteTimeUtc = (DateTime)fileNode.LastModifiedTime;
            }

            if (fileNode.FileAttr != null)
            {
                // remove default file attribute
                FileOp.RemoveFileAttribute(filePath, FileAttributes.Archive);

                // Set file Attributes
                FileOp.SetFileAttribute(filePath, (FileAttributes)fileNode.FileAttr);
                Test.Info("{0} attr is {1}", filePath, File.GetAttributes(filePath).ToString());
            }
        }

        public static FileNode GetFileNode(DirNode dirNode, params string[] tokens)
        {
            DirNode currentDirNode = dirNode;

            for (int i = 0; i < tokens.Length; ++i)
            {
                if (i == tokens.Length - 1)
                {
                    FileNode fileNode = currentDirNode.GetFileNode(tokens[i]);
                    if (fileNode == null)
                    {
                        Test.Error("FileNode {0} doesn't exist.", tokens[i]);
                        return null;
                    }

                    return fileNode;
                }
                else
                {
                    currentDirNode = currentDirNode.GetDirNode(tokens[i]);
                    if (currentDirNode == null)
                    {
                        Test.Error("DirNode {0} doesn't exist.", tokens[i]);
                        return null;
                    }
                }
            }

            return null;
        }

        public static void RemoveAllFileNodesExcept(DirNode rootNode, HashSet<FileNode> except)
        {
            List<FileNode> nodesToRemove = new List<FileNode>();
            foreach (FileNode fileNode in rootNode.EnumerateFileNodesRecursively())
            {
                if (!except.Contains(fileNode))
                {
                    nodesToRemove.Add(fileNode);
                }
            }

            foreach(FileNode nodeToRemove in nodesToRemove)
            {
                nodeToRemove.Parent.DeleteFileNode(nodeToRemove.Name);
            }
        }

        public static string DetailedInfo(this DMLibDataInfo dataInfo)
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine(string.Format("TransferDataInfo root: {0}", dataInfo.RootPath));

            foreach (FileNode fileNode in dataInfo.EnumerateFileNodes())
            {
                builder.AppendLine(fileNode.DetailedInfo());
            }

            return builder.ToString();
        }

        public static string DetailedInfo(this FileNode fileNode)
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("FileNode {0}: MD5 ({1}), LMT ({2})", fileNode.GetURLRelativePath(), fileNode.MD5, fileNode.LastModifiedTime);

            return builder.ToString();
        }

        public static bool Equals(DMLibDataInfo infoA, DMLibDataInfo infoB)
        {
            bool result;
            
            bool aIsEmpty = infoA == null || infoA.RootNode.IsEmpty;
            bool bIsEmpty = infoB == null || infoB.RootNode.IsEmpty;

            if (aIsEmpty && bIsEmpty)
            {
                result = true;
            }
            else if(aIsEmpty || bIsEmpty)
            {
                result = false;
            }
            else
            {
                result = Equals(infoA.RootNode, infoB.RootNode);
            }

            if (!result)
            {
                Test.Info("-----Data Info A-----");
                MultiDirectionTestHelper.PrintTransferDataInfo(infoA);

                Test.Info("-----Data Info B-----");
                MultiDirectionTestHelper.PrintTransferDataInfo(infoB);
            }

            return result;
        }

        public static bool Equals(DirNode dirNodeA, DirNode dirNodeB)
        {
            // The same node
            if (dirNodeA == dirNodeB)
            {
                return true;
            }

            // Empty node equals to null
            if ((dirNodeA == null || dirNodeA.IsEmpty) &&
                (dirNodeB == null || dirNodeB.IsEmpty))
            {
                return true;
            }

            // Compare two nodes
            if (null != dirNodeA && null != dirNodeB)
            {
                if (dirNodeA.FileNodeCount != dirNodeB.FileNodeCount ||
                    dirNodeA.NonEmptyDirNodeCount != dirNodeB.NonEmptyDirNodeCount)
                {
                    return false;
                }

                if ((null != dirNodeA.Metadata) && (dirNodeA.Metadata.Count > 0))
                {
                    if (null == dirNodeB.Metadata) return false;

                    if (dirNodeA.Metadata.Count != dirNodeB.Metadata.Count) return false;

                    foreach (var keyValue in dirNodeA.Metadata)
                    {
                        if (!string.Equals(dirNodeB.Metadata[keyValue.Key], keyValue.Value)) return false;
                    }
                }
                else
                {
                    if ((null != dirNodeB.Metadata) && (dirNodeB.Metadata.Count > 0)) return false;
                }

                foreach(FileNode fileNodeA in dirNodeA.FileNodes)
                {
                    FileNode fileNodeB = dirNodeB.GetFileNode(fileNodeA.Name);

                    FileNode fileNodeAA = fileNodeA;

                    if (null == fileNodeB)
                    {
                        fileNodeB = dirNodeB.GetFileNode(DMLibTestHelper.EscapeInvalidCharacters(fileNodeA.Name));

                        if (null != fileNodeB)
                        {
                            fileNodeAA = fileNodeA.Clone(DMLibTestHelper.EscapeInvalidCharacters(fileNodeA.Name));
                        }
                    }

                    if (!DMLibDataHelper.Equals(fileNodeAA, fileNodeB))
                    {
                        return false;
                    }
                }

                foreach(DirNode subDirNodeA in dirNodeA.DirNodes)
                {
                    Test.Info("Verifying subfolder: {0} ", subDirNodeA.Name);
                    DirNode subDirNodeB = dirNodeB.GetDirNode(subDirNodeA.Name);

                    if (null == subDirNodeB)
                    {
                        subDirNodeB = dirNodeB.GetDirNode(DMLibTestHelper.EscapeInvalidCharacters(subDirNodeA.Name));
                    }

                    if (!DMLibDataHelper.Equals(subDirNodeA, subDirNodeB))
                    {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        public static bool Equals(FileNode fileNodeA, FileNode fileNodeB)
        {
            if (fileNodeA == fileNodeB)
            {
                return true;
            }

            if (null != fileNodeA && null != fileNodeB)
            {
                Test.Info(string.Format("Verify file: ({0},{1}); ({2},{3})", fileNodeA.Name, fileNodeA.MD5, fileNodeB.Name, fileNodeB.MD5));

                if (!string.Equals(fileNodeA.Name, fileNodeB.Name, StringComparison.Ordinal) ||
                    !PropertiesStringEquals(fileNodeA.MD5, fileNodeB.MD5) ||
                    !PropertiesStringEquals(fileNodeA.CacheControl, fileNodeB.CacheControl) ||
                    !PropertiesStringEquals(fileNodeA.ContentDisposition, fileNodeB.ContentDisposition) ||
                    !PropertiesStringEquals(fileNodeA.ContentEncoding, fileNodeB.ContentEncoding) ||
                    !PropertiesStringEquals(fileNodeA.ContentLanguage, fileNodeB.ContentLanguage))
                {
                    Test.Info("({0}, {1})", fileNodeA.CacheControl, fileNodeB.CacheControl);
                    Test.Info("({0}, {1})", fileNodeA.ContentDisposition, fileNodeB.ContentDisposition);
                    Test.Info("({0}, {1})", fileNodeA.ContentEncoding, fileNodeB.ContentEncoding);
                    Test.Info("({0}, {1})", fileNodeA.ContentLanguage, fileNodeB.ContentLanguage);
                    return false;
                }

                if (!MetadataEquals(fileNodeA.Metadata, fileNodeB.Metadata))
                {
                    Test.Info("meta data");
                    return false;
                }

                foreach (var keyValuePair in fileNodeA.Metadata)
                {
                    if (!fileNodeB.Metadata.Contains(keyValuePair))
                    {
                        Test.Info("Meta data {0} {1}", keyValuePair.Key, keyValuePair.Value);
                        return false;
                    }
                }

                return true;
            }

            string name;
            if (fileNodeA != null)
            {
                name = fileNodeA.Name;
            }
            else
            {
                name = fileNodeB.Name;
            }

            Test.Info("Fail to verify file: {0}", name);
            return false;
        }

        private static bool MetadataEquals(IDictionary<string, string> metadataA, IDictionary<string, string> metadataB)
        {
            if (metadataA == metadataB)
            {
                return true;
            }

            if (metadataA == null || metadataB == null)
            {
                return false;
            }

            if (metadataA.Count != metadataB.Count)
            {
                return false;
            }

            foreach (var keyValuePair in metadataB)
            {
                if (!metadataB.Contains(keyValuePair))
                {
                    Test.Info("Meta data ({0}, {1})", keyValuePair.Key, keyValuePair.Value);
                    return false;
                }
            }

            return true;
        }

        private static bool PropertiesStringEquals(string valueA, string ValueB)
        {
            if (string.IsNullOrEmpty(valueA))
            {
                if (string.IsNullOrEmpty(ValueB))
                {
                    return true;
                }

                return false;
            }

            return string.Equals(valueA, ValueB, StringComparison.Ordinal);
        }

        public static string GetLocalRelativePath(this DataInfoNode node)
        {
            return Path.Combine(node.PathComponents.ToArray());
        }

        public static string GetURLRelativePath(this DataInfoNode node)
        {
            return String.Join("/", node.PathComponents);
        }

        public static string GetSourceRelativePath(this DataInfoNode node)
        {
            if (DMLibTestContext.SourceType == DMLibDataType.Local)
            {
                return node.GetLocalRelativePath();
            }
            else
            {
                return node.GetURLRelativePath();
            }
        }

        public static string GetDestRelativePath(this DataInfoNode node)
        {
            if (DMLibTestContext.DestType == DMLibDataType.Local)
            {
                return node.GetLocalRelativePath();
            }
            else
            {
                return node.GetURLRelativePath();
            }
        }

        public static void SetCalculatedFileMD5(DMLibDataInfo dataInfo, DataAdaptor<DMLibDataInfo> destAdaptor, bool disableMD5Check = false)
        {
            foreach (FileNode fileNode in dataInfo.EnumerateFileNodes())
            {
                if (DMLibTestBase.IsCloudBlob(DMLibTestContext.DestType))
                {
                    CloudBlobDataAdaptor cloudBlobDataAdaptor = destAdaptor as CloudBlobDataAdaptor;
                    CloudBlob cloudBlob = cloudBlobDataAdaptor.GetCloudBlobReference(dataInfo.RootPath, fileNode);

                    fileNode.MD5 = CloudBlobHelper.CalculateMD5ByDownloading(cloudBlob, disableMD5Check);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
                {
                    CloudFileDataAdaptor cloudFileDataAdaptor = destAdaptor as CloudFileDataAdaptor;
                    CloudFile cloudFile = cloudFileDataAdaptor.GetCloudFileReference(dataInfo.RootPath, fileNode);

                    fileNode.MD5 = CloudFileHelper.CalculateMD5ByDownloading(cloudFile, disableMD5Check);
                }

                // No need to set md5 for local destination
            }
        }
    }
}
