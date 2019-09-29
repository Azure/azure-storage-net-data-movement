//------------------------------------------------------------------------------
// <copyright file="Helpers.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Security.Cryptography;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using DMLibTest.Framework;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.RetryPolicies;
    using MS.Test.Common.MsTestLib;
    using StorageBlobType = Microsoft.Azure.Storage.Blob.BlobType;

    /// <summary>
    /// this is a static helper class
    /// </summary>
    public static class Helper
    {
        private static Random random = new Random();

        public static CloudFileNtfsAttributes ToCloudFileNtfsAttributes(FileAttributes fileAttributes)
        {
            CloudFileNtfsAttributes cloudFileNtfsAttributes = CloudFileNtfsAttributes.None;

            if ((fileAttributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.ReadOnly;
            if ((fileAttributes & FileAttributes.Hidden) == FileAttributes.Hidden)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Hidden;
            if ((fileAttributes & FileAttributes.System) == FileAttributes.System)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.System;
            if ((fileAttributes & FileAttributes.Directory) == FileAttributes.Directory)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Directory;
            if ((fileAttributes & FileAttributes.Archive) == FileAttributes.Archive)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Archive;
            if ((fileAttributes & FileAttributes.Normal) == FileAttributes.Normal)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Normal;
            if ((fileAttributes & FileAttributes.Temporary) == FileAttributes.Temporary)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Temporary;
            if ((fileAttributes & FileAttributes.Offline) == FileAttributes.Offline)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.Offline;
            if ((fileAttributes & FileAttributes.NotContentIndexed) == FileAttributes.NotContentIndexed)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.NotContentIndexed;
            if ((fileAttributes & FileAttributes.NoScrubData) == FileAttributes.NoScrubData)
                cloudFileNtfsAttributes |= CloudFileNtfsAttributes.NoScrubData;

            return cloudFileNtfsAttributes;
        }

        public static FileAttributes ToFileAttributes(CloudFileNtfsAttributes cloudFileNtfsAttributes)
        {
            FileAttributes fileAttributes = (FileAttributes)0;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.ReadOnly) == CloudFileNtfsAttributes.ReadOnly)
                fileAttributes |= FileAttributes.ReadOnly;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Hidden) == CloudFileNtfsAttributes.Hidden)
                fileAttributes |= FileAttributes.Hidden;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.System) == CloudFileNtfsAttributes.System)
                fileAttributes |= FileAttributes.System;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Directory) == CloudFileNtfsAttributes.Directory)
                fileAttributes |= FileAttributes.Directory;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Archive) == CloudFileNtfsAttributes.Archive)
                fileAttributes |= FileAttributes.Archive;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Normal) == CloudFileNtfsAttributes.Normal)
                fileAttributes |= FileAttributes.Normal;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Temporary) == CloudFileNtfsAttributes.Temporary)
                fileAttributes |= FileAttributes.Temporary;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.Offline) == CloudFileNtfsAttributes.Offline)
                fileAttributes |= FileAttributes.Offline;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.NotContentIndexed) == CloudFileNtfsAttributes.NotContentIndexed)
                fileAttributes |= FileAttributes.NotContentIndexed;

            if ((cloudFileNtfsAttributes & CloudFileNtfsAttributes.NoScrubData) == CloudFileNtfsAttributes.NoScrubData)
                fileAttributes |= FileAttributes.NoScrubData;

            return fileAttributes;
        }

        public static void CompareSMBProperties(DirNode dirNode0, DirNode dirNode1, bool compareFileAttributes)
        {
            Test.Assert(string.Equals(dirNode0.Name, dirNode1.Name), "Directory name should be the expected");

            if (dirNode0.LastWriteTime.HasValue)
            {
                if (!dirNode1.LastWriteTime.HasValue
                    || (dirNode0.LastWriteTime.Value != dirNode1.LastWriteTime.Value))
                {
                    Test.Error("lastwritetime for {0} is not expected.", dirNode0.Name);
                }
            }

            if (dirNode0.CreationTime.HasValue)
            {
                if (!dirNode1.CreationTime.HasValue
                    || (dirNode0.CreationTime.Value != dirNode1.CreationTime.Value))
                {
                    Test.Error("CreationTime for {0} is not expected.", dirNode0.Name);
                }
            }

            if (compareFileAttributes)
            {
                if (dirNode0.SMBAttributes.HasValue)
                {
                    if (!dirNode1.SMBAttributes.HasValue
                        || (dirNode0.SMBAttributes.Value != dirNode1.SMBAttributes.Value))
                    {
                        Test.Error("SMBAttributes for {0} is not expected.", dirNode0.Name);
                    }
                }
            }

            foreach (var fileNode in dirNode0.FileNodes)
            {
                var fileNode1 = dirNode1.GetFileNode(fileNode.Name);

                if (null == fileNode1)
                {
                    Test.Error("File node mismatch");
                    continue;
                }

                if ((fileNode.LastWriteTime.Value != fileNode1.LastWriteTime.Value)
                    || (fileNode.CreationTime.Value != fileNode1.CreationTime.Value))
                {
                    Test.Error("File node mismatch");
                }

                if (compareFileAttributes
                    && fileNode.SMBAttributes.Value != fileNode1.SMBAttributes.Value)
                {
                    Test.Error("File node mismatch");
                }
            }

            foreach (var subDirNode in dirNode0.DirNodes)
            {
                var subDirNode1 = dirNode1.GetDirNode(subDirNode.Name);

                if (null == subDirNode1)
                {
                    Test.Error("DirNode mismatch {0}", subDirNode.Name);
                }
                else
                {
                    CompareSMBProperties(subDirNode, subDirNode1, compareFileAttributes);
                }
            }
        }

        public static void CopyLocalDirectory(string sourceDir, string destDir, bool recursive)
        {
            if (!LongPathDirectoryExtension.Exists(destDir))
            {
                LongPathDirectoryExtension.CreateDirectory(destDir);
            }

            foreach (var subDir in Directory.GetDirectories(sourceDir, "*", recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly))
            {
                LongPathDirectoryExtension.CreateDirectory(ConvertSourceToDestPath(sourceDir, destDir, subDir));
            }

            foreach (var file in Directory.GetFiles(sourceDir, "*", recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly))
            {
                File.Copy(file, ConvertSourceToDestPath(sourceDir, destDir, file));
            }
        }

        private static string ConvertSourceToDestPath(string sourceRoot, string destRoot, string path)
        {
            int index = path.IndexOf(sourceRoot, StringComparison.OrdinalIgnoreCase);
            string relativePath = path.Substring(index + sourceRoot.Length);

            if (relativePath.StartsWith(Path.DirectorySeparatorChar.ToString()))
            {
                relativePath = relativePath.Substring(1);
            }

            return Path.Combine(destRoot, relativePath);
        }

        /// <summary>
        /// list blobs in a container, return blob name list and content MD5 list
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobList"></param>
        /// <returns></returns>
        public static bool ListBlobs(string connectionString, string containerName, out List<string> blobNames, out List<string> blobMD5s)
        {
            CloudBlobClient BlobClient = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.Zero, 3);

            blobNames = new List<string>();
            blobMD5s = new List<string>();

            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                IEnumerable<IListBlobItem> blobs = container.ListBlobs(null, true, BlobListingDetails.All, HelperConst.DefaultBlobOptions);
                if (blobs != null)
                {
                    foreach (CloudBlob blob in blobs)
                    {
                        blob.FetchAttributes();
                        blobNames.Add(blob.Name);
                        blobMD5s.Add(blob.Properties.ContentMD5);
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public static void WaitForTakingEffect(dynamic cloudStorageClient)
        {
            Test.Assert(
                cloudStorageClient is CloudBlobClient || cloudStorageClient is CloudFileClient,
                "The argument should only be CloudStorageClient.");

            if (DMLibTestHelper.GetTestAgainst() != TestAgainst.PublicAzure)
            {
                return;
            }

            DateTimeOffset? lastSyncTime = cloudStorageClient.GetServiceStats().GeoReplication.LastSyncTime;
            DateTimeOffset currentTime = DateTimeOffset.UtcNow;
            int maxWaitCount = 120;

            DateTimeOffset? newLastSyncTime = cloudStorageClient.GetServiceStats().GeoReplication.LastSyncTime;

            while ((maxWaitCount > 0)
                && (!newLastSyncTime.HasValue || (lastSyncTime.HasValue && newLastSyncTime.Value <= lastSyncTime.Value) || newLastSyncTime.Value <= currentTime))
            {
                --maxWaitCount;
                Test.Info("Waiting......");
                Thread.Sleep(10000);
                newLastSyncTime = cloudStorageClient.GetServiceStats().GeoReplication.LastSyncTime;
            }

            if (maxWaitCount <= 0)
            {
                Test.Info("NOTE: Wait for taking effect timed out, cases may fail...");
            }
        }

        public static bool RandomBoolean()
        {
            return random.Next(0, 2) % 2 == 0;
        }

        public static string RandomBlobType()
        {
            int rnd = random.Next(0, 3);
            if (rnd == 0)
            {
                return BlobType.Block;
            }
            else if (rnd == 1)
            {
                return BlobType.Page;
            }
            else
            {
                return BlobType.Append;
            }
        }

        public static void GenerateFileInBytes(string filename, long sizeB)
        {
            Random r = new Random();
            byte[] data;

            using (FileStream stream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                var oneMBInBytes = 1024 * 1024;
                var sizeInMB = sizeB / oneMBInBytes;
                data = new byte[oneMBInBytes];
                for (int i = 0; i < sizeInMB; i++)
                {
                    r.NextBytes(data);
                    stream.Write(data, 0, data.Length);
                }

                var restSizeInB = sizeB % oneMBInBytes;
                data = new byte[restSizeInB];
                r.NextBytes(data);
                stream.Write(data, 0, data.Length);
            }
        }

        public static void GenerateFileInKB(string filename, long sizeKB)
        {
            byte[] data = new byte[sizeKB * 1024];
            Random r = new Random();
            r.NextBytes(data);
            File.WriteAllBytes(filename, data);
            return;
        }

        //it takes around 74 seconds to generate a 5G file
        public static void GenerateFileInMB(string filename, long sizeMB)
        {
            byte[] data = new byte[1024 * 1024];
            Random r = new Random();
            using (FileStream stream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                for (int i = 0; i < sizeMB; i++)
                {
                    r.NextBytes(data);
                    stream.Write(data, 0, data.Length);
                }
            }

            return;
        }

        // the buffer is too large, better to use GenerateMediumFile
        public static void GenerateFileInGB(string filename, long sizeGB)
        {
            byte[] data = new byte[4 * 1024 * 1024];
            long chunkCount = 256 * sizeGB;
            Random r = new Random();
            using (FileStream stream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                for (int i = 0; i < chunkCount; i++)
                {
                    r.NextBytes(data);
                    stream.Write(data, 0, data.Length);
                }
            }

            return;
        }

        public static void GenerateEmptyFile(string filename)
        {
            if (LongPathFileExtension.Exists(filename))
            {
                Test.Info("GenerateEmptyFile: delte existing file");
                LongPathFileExtension.Delete(filename);
            }

            using (FileStream file = LongPathFileExtension.Create(filename))
            {
            }
        }

        public static void AggregateFile(string filename, int times)
        {
            using (FileStream outputStream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                using (FileStream inputStream = LongPathFileExtension.Open("abc.txt", FileMode.Open))
                {
                    for (int i = 0; i < times; i++)
                    {
                        inputStream.CopyTo(outputStream);
                        inputStream.Seek(0, SeekOrigin.Begin);
                    }
                }
            }
        }

        public static void CompressFile(string filename, int times)
        {
            using (FileStream outputStream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                using (GZipStream compress = new GZipStream(outputStream, CompressionMode.Compress))
                {

                    using (FileStream inputStream = LongPathFileExtension.Open("abc.txt", FileMode.Open))
                    {
                        for (int i = 0; i < times; i++)
                        {
                            inputStream.CopyTo(compress);
                            inputStream.Seek(0, SeekOrigin.Begin);
                        }
                    }
                }
            }
        }

        public static void GenerateLargeFileinKB(string filename, long sizeinKB)
        {
            byte[] data4MB = new byte[4 * 1024 * 1024];
            byte[] dataMB = new byte[1024 * 1024];
            Random r = new Random();
            using (FileStream stream = LongPathFileExtension.Open(filename, FileMode.Create))
            {
                long sizeGB = sizeinKB / (1024 * 1024);
                long sizeMB = sizeinKB % (1024 * 1024) / 1024;
                long sizeKB = sizeinKB % 1024;
                for (long i = 0; i < sizeGB * 256; i++)
                {
                    r.NextBytes(data4MB);
                    stream.Write(data4MB, 0, data4MB.Length);
                }
                for (long i = 0; i < sizeMB; i++)
                {
                    r.NextBytes(dataMB);
                    stream.Write(dataMB, 0, dataMB.Length);
                }
                if (sizeKB != 0)
                {
                    byte[] dataKB = new byte[sizeKB * 1024];
                    r.NextBytes(dataKB);
                    stream.Write(dataKB, 0, dataKB.Length);
                }
            }

            return;
        }

        //this is only for small data 
        public static byte[] GetMD5(byte[] data)
        {
            MD5 md5 = MD5.Create();
            return md5.ComputeHash(data);
        }

        public static void GenerateRandomTestFile(string filename, long sizeKB, bool createDirIfNotExist = false)
        {
            if (createDirIfNotExist)
            {
                string dir = Path.GetDirectoryName(filename);
                if (!string.IsNullOrEmpty(dir))
                {
                    LongPathDirectoryExtension.CreateDirectory(dir);
                }
            }

            byte[] data = new byte[sizeKB * 1024];
            Random r = new Random();
            r.NextBytes(data);
            File.WriteAllBytes(filename, data);
        }

        public static void DeleteFile(string filename)
        {
            if (File.Exists(filename))
            {
                LongPathFileExtension.Delete(filename);
            }
        }

        public static void CleanupFolder(string foldername)
        {
            if (!LongPathDirectoryExtension.Exists(foldername))
            {
                return;
            }

            foreach (string fileName in LongPathDirectoryExtension.GetFiles(foldername))
            {
                ForceDeleteFile(fileName);
            }

            foreach (string subFolderName in LongPathDirectoryExtension.GetDirectories(foldername))
            {
                ForceDeleteFiles(subFolderName);
            }
        }

        public static void DeleteFolder(string foldername)
        {
            if (LongPathDirectoryExtension.Exists(foldername))
            {
                ForceDeleteFiles(foldername);
            }
        }

        private static void ForceDeleteFile(string filename)
        {
            try
            {
                LongPathFileExtension.Delete(filename);
            }
            catch
            {
                FileOp.SetFileAttribute(filename, FileAttributes.Normal);
                LongPathFileExtension.Delete(filename);
            }
        }

        private static void ForceDeleteFiles(string foldername)
        {
            try
            {
                LongPathDirectoryExtension.Delete(foldername, true);
            }
            catch (Exception)
            {
                RecursiveRemoveReadOnlyAttribute(foldername);
                LongPathDirectoryExtension.Delete(foldername, true);
            }
        }

        private static void RecursiveRemoveReadOnlyAttribute(string foldername)
        {
            foreach (string filename in LongPathDirectoryExtension.GetFiles(foldername))
            {
                FileOp.SetFileAttribute(filename, FileAttributes.Normal);
            }

            foreach (string folder in LongPathDirectoryExtension.GetDirectories(foldername))
            {
                RecursiveRemoveReadOnlyAttribute(folder);
            }
        }

        public static void DeletePattern(string pathPattern)
        {
            DirectoryInfo folder = new DirectoryInfo(".");
            foreach (FileInfo fi in folder.GetFiles(pathPattern, SearchOption.TopDirectoryOnly))
            {
                fi.Delete();
            }
            foreach (DirectoryInfo di in folder.GetDirectories(pathPattern, SearchOption.TopDirectoryOnly))
            {
                di.Delete(true);
            }
        }

        public static void CreateNewFolder(string foldername)
        {
            if (LongPathDirectoryExtension.Exists(foldername))
            {
                LongPathDirectoryExtension.Delete(foldername, true);
            }
            if (LongPathFileExtension.Exists(foldername))
            {
                LongPathFileExtension.Delete(foldername);
            }

            LongPathDirectoryExtension.CreateDirectory(foldername);
        }

        // for a 5G file, this can be done in 20 seconds
        public static string GetFileMD5Hash(string filename)
        {
            using (FileStream fs = LongPathFileExtension.Open(filename, FileMode.Open))
            {
                MD5 md5 = MD5.Create();
                byte[] md5Hash = md5.ComputeHash(fs);


                StringBuilder sb = new StringBuilder();
                foreach (byte b in md5Hash)
                {
                    sb.Append(b.ToString("x2").ToLower());
                }

                return sb.ToString();
            }
        }

        public static string GetFileContentMD5(string filename)
        {
            using (FileStream fs = LongPathFileExtension.Open(filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                MD5 md5 = MD5.Create();
                byte[] md5Hash = md5.ComputeHash(fs);

                return Convert.ToBase64String(md5Hash);
            }
        }

        public static List<string> GenerateFlatTestFolder(string fileNamePrefix, string parentDir, int fileSizeInKB = -1, bool doNotGenerateFile = false)
        {
            return GenerateFixedTestTree(fileNamePrefix, string.Empty, parentDir, DMLibTestConstants.FlatFileCount, 0, fileSizeInKB, doNotGenerateFile);
        }

        public static List<string> GenerateRecursiveTestFolder(string fileNamePrefix, string dirNamePrefix, string parentDir, int fileSizeInKB = -1, bool doNotGenerateFile = false)
        {
            return GenerateFixedTestTree(fileNamePrefix, dirNamePrefix, parentDir, DMLibTestConstants.RecursiveFolderWidth, DMLibTestConstants.RecursiveFolderDepth, fileSizeInKB, doNotGenerateFile);
        }

        public static List<string> GenerateFixedTestTree(string fileNamePrefix, string dirNamePrefix, string parentDir, int width, int depth, int fileSizeInKB = -1, bool doNotGenerateFile = false)
        {
            var fileList = new List<string>();
            for (int i = 0; i < width; i++)
            {
                var fileName = parentDir + Path.DirectorySeparatorChar + fileNamePrefix + "_" + i;
                fileList.Add(fileName);
                if (!doNotGenerateFile)
                {
                    GenerateRandomTestFile(fileName, fileSizeInKB < 0 ? i : fileSizeInKB);
                }
            }

            if (depth > 0)
            {
                for (int i = 0; i < width; i++)
                {
                    string dirName = parentDir + Path.DirectorySeparatorChar + dirNamePrefix + "_" + i;

                    if (!doNotGenerateFile)
                    {
                        LongPathDirectoryExtension.CreateDirectory(dirName);
                    }

                    fileList.AddRange(GenerateFixedTestTree(fileNamePrefix, dirNamePrefix, dirName, width, depth - 1, fileSizeInKB, doNotGenerateFile));
                }
            }

            return fileList;
        }

        public static List<string> TraversalFolderInDepth(string folderName)
        {
            List<string> files = new List<string>();
            Stack<string> dirStack = new Stack<string>();
            dirStack.Push(folderName);

            while (dirStack.Count > 0)
            {
                string currentFolder = dirStack.Pop();

                foreach (string file in Directory.EnumerateFiles(currentFolder))
                {
                    files.Add(file);
                }

                Stack<string> foldersUnderCurrent = new Stack<string>();

                foreach (string folder in Directory.EnumerateDirectories(currentFolder))
                {
                    foldersUnderCurrent.Push(folder);
                }

                foreach (string folderPath in foldersUnderCurrent)
                {
                    dirStack.Push(folderPath);
                }
            }

            return files;
        }

        public static void CompareBlobAndFile(string filename, CloudBlob blob)
        {
            string tempblob = "tempblob";
            DeleteFile(tempblob);
            try
            {
                if (!File.Exists(filename))
                    Test.Error("The file {0} should exist", filename);
                if (blob == null)
                    Test.Error("The blob {0} should exist", blob.Name);
                using (FileStream fs = LongPathFileExtension.Open(tempblob, FileMode.Create))
                {
                    BlobRequestOptions bro = new BlobRequestOptions();
                    bro.RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 30), 3);
                    bro.ServerTimeout = new TimeSpan(1, 30, 0);
                    bro.MaximumExecutionTime = new TimeSpan(1, 30, 0);
                    blob.DownloadToStream(fs, null, bro);
#if DNXCORE50
                    fs.Dispose();
#else
                    fs.Close();
#endif
                }
                string MD51 = Helper.GetFileContentMD5(tempblob);
                string MD52 = Helper.GetFileContentMD5(filename);

                if (MD51 != MD52)
                    Test.Error("{2}: {0} == {1}", MD51, MD52, filename);
                DeleteFile(tempblob);
            }
            catch (Exception e)
            {
                Test.Error("Meet Excpetion when download and compare blob {0}, file{1}, Excpetion: {2}", blob.Name, filename, e.ToString());
                DeleteFile(tempblob);
                return;
            }
        }

        public static bool CompareTwoFiles(string filename, string filename2)
        {
            FileInfo fi = new FileInfo(filename);
            FileInfo fi2 = new FileInfo(filename2);
            return CompareTwoFiles(fi, fi2);
        }

        public static bool CompareTwoFiles(FileInfo fi, FileInfo fi2)
        {
            if (!fi.Exists || !fi2.Exists)
            {
                return false;
            }
            if (fi.Length != fi2.Length)
            {
                return false;
            }

            long fileLength = fi.Length;
            // 4M a chunk
            const int ChunkSizeByte = 4 * 1024 * 1024;
            using (FileStream fs = LongPathFileExtension.Open(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using (FileStream fs2 = LongPathFileExtension.Open(fi2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    BinaryReader reader = new BinaryReader(fs);
                    BinaryReader reader2 = new BinaryReader(fs2);

                    long comparedLength = 0;
                    do
                    {
                        byte[] bytes = reader.ReadBytes(ChunkSizeByte);
                        byte[] bytes2 = reader2.ReadBytes(ChunkSizeByte);

                        MD5 md5 = MD5.Create();
                        byte[] md5Hash = md5.ComputeHash(bytes);
                        byte[] md5Hash2 = md5.ComputeHash(bytes2);

                        if (!md5Hash.SequenceEqual(md5Hash2))
                        {
                            return false;
                        }

                        comparedLength += bytes.Length;
                    } while (comparedLength < fileLength);
                }
            }

            return true;
        }

        public static bool CompareTwoFolders(string foldername, string foldername2, bool recursive = true)
        {
            DirectoryInfo folder = new DirectoryInfo(foldername);
            DirectoryInfo folder2 = new DirectoryInfo(foldername2);

            IEnumerable<FileInfo> list = folder.GetFiles("*.*", recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);
            IEnumerable<FileInfo> list2 = folder2.GetFiles("*.*", recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);

            FileCompare fc = new FileCompare();

            return list.SequenceEqual(list2, fc);
        }

        public static bool CompareFolderWithBlob(string foldername, string containerName)
        {
            return true;
        }

        public static bool CompareTwoBlobs(string containerName, string containerName2)
        {
            return false; //todo: implement
        }

        public static string[] ListToGetRelativePaths(string folderName)
        {
            DirectoryInfo folder = new DirectoryInfo(folderName);
            IEnumerable<FileInfo> list = folder.GetFiles("*.*", SearchOption.AllDirectories);
            List<string> relativePaths = new List<string>();

            string absolutePath = folder.FullName + Path.DirectorySeparatorChar;

            foreach (FileInfo fileInfo in list)
            {
                relativePaths.Add(fileInfo.FullName.Substring(absolutePath.Length, fileInfo.FullName.Length - absolutePath.Length));
            }

            return relativePaths.ToArray();
        }

        public static void verifyFilesExistinBlobDirectory(int fileNumber, CloudBlobDirectory blobDirectory, string FileName, String blobType)
        {
            for (int i = 0; i < fileNumber; i++)
            {
                string blobName = FileName + "_" + i;
                CloudBlob blob = blobDirectory.GetBlobReference(blobName);
                if (null == blob || !blob.Exists())
                {
                    Test.Error("the file {0} in the blob virtual directory does not exist:", blobName);
                }
            }
        }

        public static void VerifyFilesExistInFileDirectory(int fileNumber, CloudFileDirectory fileDirectory, string fileName)
        {
            for (int i = 0; i < fileNumber; i++)
            {
                CloudFile cloudFile = fileDirectory.GetFileReference(fileName + "_" + i);
                if (null == cloudFile || !cloudFile.Exists())
                    Test.Error("the file {0}_{1} in the directory does not exist:", fileName, i);
            }
        }

        /// <summary>
        /// calculate folder size in Byte
        /// </summary>
        /// <param name="folder">the folder path</param>
        /// <returns>the folder size in Byte</returns>
        public static long CalculateFolderSizeInByte(string folder)
        {
            long folderSize = 0;
            try
            {
                //Checks if the path is valid or not
                if (!LongPathDirectoryExtension.Exists(folder))
                    return folderSize;
                else
                {
                    try
                    {
                        foreach (string file in LongPathDirectoryExtension.GetFiles(folder))
                        {
                            if (File.Exists(file))
                            {
                                FileInfo finfo = new FileInfo(file);
                                folderSize += finfo.Length;
                            }
                        }

                        foreach (string dir in LongPathDirectoryExtension.GetDirectories(folder))
                            folderSize += CalculateFolderSizeInByte(dir);
                    }
                    catch (NotSupportedException e)
                    {
                        Test.Error("Unable to calculate folder size: {0}", e.Message);
                        throw;
                    }
                }
            }
            catch (UnauthorizedAccessException e)
            {
                Test.Error("Unable to calculate folder size: {0}", e.Message);
                throw;
            }

            return folderSize;
        }

        /// <summary>
        /// Count number of files in the folder
        /// </summary>
        /// <param name="folder">the folder path</param>
        /// <param name="recursive">whether including subfolders recursively or not</param>
        /// <returns>number of files under the folder (and subfolders)</returns>
        public static int GetFileCount(string folder, bool recursive)
        {
            int count = 0;
            try
            {
                //Checks if the path is valid or not
                if (LongPathDirectoryExtension.Exists(folder))
                {
                    count += LongPathDirectoryExtension.GetFiles(folder).Length;

                    if (recursive)
                    {
                        foreach (string dir in LongPathDirectoryExtension.GetDirectories(folder))
                            count += GetFileCount(dir, true);
                    }
                }
            }
            catch (NotSupportedException e)
            {
                Test.Error("Exception thrown when accessing folder: {0}", e.Message);
                throw;
            }
            catch (UnauthorizedAccessException e)
            {
                Test.Error("Exception thrown when accessing folder: {0}", e.Message);
                throw;
            }

            return count;
        }

        public static Process StartProcess(string cmd, string args)
        {
            Test.Info("Running: {0} {1}", cmd, args);
            ProcessStartInfo psi = new ProcessStartInfo(cmd, args);
            psi.CreateNoWindow = false;
            psi.UseShellExecute = false;
            Process p = Process.Start(psi);
            return p;
        }

        public static bool WaitUntilFileCreated(string fileName, int timeoutInSeconds, bool checkContent = true)
        {
            int i = 0;
            while (i < timeoutInSeconds)
            {
                FileInfo f = new FileInfo(fileName);

                // wait for file size > 0
                if (f.Exists)
                {
                    if (!checkContent || f.Length > 0)
                    {
                        return true;
                    }
                }

                Test.Info("waiting file '{0}' to be created...", fileName);
                Thread.Sleep(1000);
                i++;
            }

            return false;
        }

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.StdCall)]
        static extern bool GenerateConsoleCtrlEvent(ConsoleCtrlEvent sigevent, int dwProcessGroupId);

#if DNXCORE50
        [DllImport("kernel32.dll", CharSet = CharSet.Ansi)]
#else
        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
#endif
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);
        public delegate bool HandlerRoutine(ConsoleCtrlEvent CtrlType);

        // An enumerated type for the control messages
        // sent to the handler routine.
        public enum ConsoleCtrlEvent
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }

        public static Process StartProcess(string cmd, string args, out StreamReader stdout, out StreamReader stderr, out StreamWriter stdin)
        {
            Test.Logger.Verbose("Running: {0} {1}", cmd, args);
            ProcessStartInfo psi = new ProcessStartInfo(cmd, args);
            psi.CreateNoWindow = true;
#if !DNXCORE50
            psi.WindowStyle = ProcessWindowStyle.Hidden;
#endif
            psi.UseShellExecute = false;
            psi.RedirectStandardError = true;
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardInput = true;
            Process p = Process.Start(psi);
            stdout = p.StandardOutput;
            stderr = p.StandardError;
            stdin = p.StandardInput;
            return p;
        }

        public static Process StartProcess(string cmd, string args, out StringBuilder stdout, out StringBuilder stderr, out StreamWriter stdin, Dictionary<string, string> faultInjectionPoints = null)
        {
            Test.Logger.Verbose("Running: {0} {1}", cmd, args);
            ProcessStartInfo psi = new ProcessStartInfo(cmd, args);
            psi.CreateNoWindow = true;
#if !DNXCORE50
            psi.WindowStyle = ProcessWindowStyle.Hidden;
#endif
            psi.UseShellExecute = false;
            psi.RedirectStandardError = true;
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardInput = true;
#if !DNXCORE50
            if (null != faultInjectionPoints)
            {
                foreach (var kv in faultInjectionPoints)
                {
                    Test.Info("Envrionment {0}:{1}", kv.Key, kv.Value);
                    psi.EnvironmentVariables.Add(kv.Key, kv.Value);
                }
            }
#endif

            Process p = Process.Start(psi);

            StringBuilder outString = new StringBuilder();
            p.OutputDataReceived += (sendingProcess, outLine) =>
            {
                if (!String.IsNullOrEmpty(outLine.Data))
                {
                    outString.Append(outLine.Data + "\n");
                }
            };

            StringBuilder errString = new StringBuilder();
            p.ErrorDataReceived += (sendingProcess, outLine) =>
            {
                if (!String.IsNullOrEmpty(outLine.Data))
                {
                    errString.Append(outLine.Data + "\n");
                }
            };
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();

            stdout = outString;
            stderr = errString;
            stdin = p.StandardInput;

            return p;
        }

        public static void PrintBlockBlobBlocks(CloudBlockBlob cloudBlob, bool printDetailBlock = true)
        {
            IEnumerable<ListBlockItem> blocks = cloudBlob.DownloadBlockList();

            Test.Info("There are {0} blocks in blob {1}: ", blocks.Count(), cloudBlob.Name);

            if (printDetailBlock)
            {
                foreach (var block in blocks)
                {
                    Test.Info("BlockId:{0}, Length:{1}", block.Name, block.Length);
                }
            }
        }

        public static void PrintPageBlobRanges(CloudPageBlob cloudBlob, bool printDetailPage = true)
        {
            //Write out the page ranges for the page blob.
            IEnumerable<PageRange> ranges = cloudBlob.GetPageRanges(options: HelperConst.DefaultBlobOptions);

            Test.Info("There are {0} pages range in blob {1}: ", ranges.Count(), cloudBlob.Name);
            if (printDetailPage)
            {
                PrintRanges(ranges);
            }
        }

        public static void PrintCloudFileRanges(CloudFile cloudFile, bool printDetailRange = true)
        {
            IEnumerable<FileRange> ranges = cloudFile.ListRanges(options: HelperConst.DefaultFileOptions);

            Test.Info("There are {0} ranges in cloud file {1}: ", ranges.Count(), cloudFile.Name);
            if (printDetailRange)
            {
                PrintRanges(ranges);
            }
        }

        private static void PrintRanges(IEnumerable<dynamic> ranges)
        {
            foreach (var range in ranges)
            {
                Test.Info(" [{0}-{1}]: {2} ", range.StartOffset, range.EndOffset, range.EndOffset - range.StartOffset + 1);
            }
        }

        public static char GenerateRandomDelimiter()
        {
            List<char> notavailList = new List<char>(new char[] { 't', 'e', 's', 'f', 'o', 'l', 'd', 'r', 'i', '\\', '?' });
            Random rnd = new Random();
            int random;
            do
            {
                random = rnd.Next(0x20, 0xFF);
            }
#if DNXCORE50
            while (CharUnicodeInfo.GetUnicodeCategory((char)random) == UnicodeCategory.Control || notavailList.Contains((char)random));
#else
            while (char.GetUnicodeCategory((char)random) == UnicodeCategory.Control || notavailList.Contains((char)random));
#endif

            return (char)random;
        }

        public static string GetAccountNameFromConnectString(string connectString)
        {
            Dictionary<string, string> dict = connectString.Split(';')
                 .Select(s => s.Split('='))
                 .ToDictionary(key => key[0].Trim(), value => value[1].Trim());

            return dict["AccountName"];
        }

        public static string GetBlobDirectoryUri(string blobEndpoint, string containerName, string dirName)
        {
            string containerUri = string.Format("{0}/{1}", blobEndpoint, containerName);
            var containerRef = new CloudBlobContainer(new Uri(containerUri));
            return containerRef.GetDirectoryReference(dirName).Uri.ToString();
        }

        public static string GetXsmbDirectoryUri(string fileEndpoint, string shareName, string dirName)
        {
            string shareUri = string.Format("{0}/{1}", fileEndpoint, shareName);
            var shareRef = new CloudFileShare(new Uri(shareUri), new StorageCredentials());
            return shareRef.GetRootDirectoryReference().GetDirectoryReference(dirName).Uri.ToString();
        }

        public static string AppendSlash(string input)
        {
            if (input.EndsWith("/"))
            {
                return input;
            }
            else
            {
                return input + "/";
            }
        }

        public static bool IsNotFoundStorageException(Exception e)
        {
            return IsStorageExceptionWithStatusCode(e, 404);
        }

        public static bool IsConflictStorageException(Exception e)
        {
            return IsStorageExceptionWithStatusCode(e, 409);
        }

        private static bool IsStorageExceptionWithStatusCode(Exception e, int statusCode)
        {
            var se = e as StorageException ?? e.InnerException as StorageException;

            return se?.RequestInformation.HttpStatusCode == statusCode;
        }

        public static void GenerateSparseCloudObject(
            List<int> ranges,
            List<int> gaps,
            Action<int> createObject,
            Action<int, Stream> writeUnit)
        {
            if (ranges.Count != gaps.Count + 1)
            {
                Test.Error("Invalid input for SparseCloudObject.");
            }

            Test.Info("Ranges:");
            ranges.PrintAllElements<int>();
            Test.Info("Gaps:");
            gaps.PrintAllElements<int>();

            int totalSize = ranges.Sum() + gaps.Sum();
            createObject(totalSize);

            int offset = 0;
            for (int i = 0; i < ranges.Count; ++i)
            {
                int range = ranges[i];

                Helper.WriteRange(offset, range, writeUnit);

                offset += range;

                if (i != ranges.Count - 1)
                {
                    offset += gaps[i];
                }
            }
        }

        private static void WriteRange(int offset, int length, Action<int, Stream> writeUnit)
        {
            int remainingLength = length;
            int currentOffset = offset;
            const int MaxLength = 4 * 1024 * 1024;

            while (remainingLength > 0)
            {
                int lengthToWrite = Math.Min(MaxLength, remainingLength);

                using (MemoryStream randomData = Helper.GetRandomData(lengthToWrite))
                {
                    writeUnit(currentOffset, randomData);
                }

                currentOffset += lengthToWrite;
                remainingLength -= lengthToWrite;
            }
        }

        public static MemoryStream GetRandomData(int size)
        {
            Random random = new Random();
            byte[] data = new byte[size];
            random.NextBytes(data);
            return new MemoryStream(data);
        }

        public static void Shuffle<T>(this List<T> list)
        {
            Random random = new Random();
            int currentPosition = list.Count;
            while (currentPosition > 1)
            {
                currentPosition--;
                int swapPosition = random.Next(currentPosition + 1);
                var temp = list[swapPosition];
                list[swapPosition] = list[currentPosition];
                list[currentPosition] = temp;
            }
        }

        public static void PrintAllElements<T>(this List<T> list)
        {
            Test.Info("[{0}]", string.Join(",", list));
        }

        /// <summary>
        /// return setting from testData.xml if exist, else return default value
        /// </summary>
        /// <param name="settingName">the name of the setting</param>
        /// <param name="defaultValue">the default Value of the setting</param>
        /// <returns>the setting value</returns>
        public static string ParseSetting(string settingName, object defaultValue)
        {
            try
            {
                return Test.Data.Get(settingName);
            }
            catch
            {
                return defaultValue.ToString();
            }
        }

        public static void VerifyCancelException(Exception e)
        {
            Test.Assert(e.Message.Contains("cancel") || (e.InnerException != null && e.InnerException.Message.Contains("cancel")), 
                "Verify task is canceled: {0}", e.Message + (e.InnerException != null? " --> " + e.InnerException.Message : string.Empty));
        }
    }

    public class FileCompare : IEqualityComparer<FileInfo>
    {
        public FileCompare() { }

        public bool Equals(FileInfo f1, FileInfo f2)
        {
            if (f1.Name != f2.Name)
            {
                Test.Verbose("file name {0}:{1} not equal {2}:{3}", f1.FullName, f1.Name, f2.FullName, f2.Name);
                return false;
            }

            if (f1.Length != f2.Length)
            {
                Test.Verbose("file length {0}:{1} not equal {2}:{3}", f1.FullName, f1.Length, f2.FullName, f2.Length);
                return false;
            }

            if (f1.Length < 200 * 1024 * 1024)
            {
                string f1MD5Hash = f1.MD5Hash();
                string f2MD5Hash = f2.MD5Hash();
                if (f1MD5Hash != f2MD5Hash)
                {
                    Test.Verbose("file MD5 mismatch {0}:{1} not equal {2}:{3}", f1.FullName, f1MD5Hash, f2.FullName, f2MD5Hash);
                    return false;
                }
            }
            else
            {
                if (!Helper.CompareTwoFiles(f1, f2))
                {
                    Test.Verbose("file MD5 mismatch {0} not equal {1}", f1.FullName, f2.FullName);
                    return false;
                }
            }
            return true;
        }

        public int GetHashCode(FileInfo fi)
        {
            string s = String.Format("{0}{1}", fi.Name, fi.Length);
            return s.GetHashCode();
        }
    }

    public static class FileOp
    {
        private const int NumberBase = 48;
        private const int UpperCaseLetterBase = 65;
        private const int Underline = 95;
        private const int LowerCaseLetterBase = 97;

        public static HashSet<char> AllSpecialChars { get; private set; }

        public static HashSet<char> InvalidCharsInLocalAndCloudFileName { get; private set; }

        public static HashSet<char> InvalidCharsInBlobName { get; private set; }

        public static List<char> ValidSpecialCharsInLocalFileName { get; private set; }

        static FileOp()
        {
            AllSpecialChars = new HashSet<char> { '$', '&', '+', ',', '/', ':', '=', '?', '@', ' ', '"', '<', '>', '#', '%', '{', '}', '|', '\\', '^', '~', '[', ']', '`', '*', '!', '(', ')', '-', '_', '\'', '.', ';' };
            InvalidCharsInLocalAndCloudFileName = new HashSet<char> { '/', ':', '?', '"', '<', '>', '|', '\\', '*' };
            InvalidCharsInBlobName = new HashSet<char> { '\\' };

            ValidSpecialCharsInLocalFileName = new List<char>();
            foreach (var ch in AllSpecialChars)
            {
                if (!InvalidCharsInLocalAndCloudFileName.Contains(ch))
                {
                    ValidSpecialCharsInLocalFileName.Add(ch);
                }
            }
        }

        public static string MD5Hash(this FileInfo fi)
        {
            return Helper.GetFileMD5Hash(fi.FullName);
        }

        public static string NextString(Random Randomint)
        {
            int length = Randomint.Next(1, 100);
            return NextString(Randomint, length);
        }

        public static string NextString(Random Randomint, int length)
        {
            if (length == 0)
            {
                return string.Empty;
            }

            while (true)
            {
                var result = new String(
                    Enumerable.Repeat(0, length)
                        .Select(p => GetRandomVisiableChar(Randomint))
                        .ToArray());
                result = result.Trim();
                if (result.Length == length && !string.IsNullOrWhiteSpace(result) && !result.EndsWith("."))
                {
                    return result;
                }
            }
        }

        public static string NextCIdentifierString(Random random)
        {
            int length = random.Next(1, 255);
            return NextCIdentifierString(random, length);
        }

        public static string NextCIdentifierString(Random random, int length)
        {
            char[] charArray =
                Enumerable.Repeat(0, length)
                    .Select(p => GetCIdentifierChar(random))
                    .ToArray();

            if (charArray[0] >= NumberBase && charArray[0] <= (NumberBase + 10))
            {
                charArray[0] = '_';
            }

            return new string(charArray);
        }

        public static string NextNormalString(Random random)
        {
            int length = random.Next(1, 255);
            return NextNormalString(random, length);
        }

        public static string NextNormalString(Random random, int length)
        {
            while (true)
            {
                var result = new String(
                    Enumerable.Repeat(0, length)
                        .Select(p => GetNormalChar(random))
                        .ToArray());
                result = result.Trim();
                if (result.Length == length && !string.IsNullOrWhiteSpace(result) && !result.EndsWith("."))
                {
                    return result;
                }
            }
        }

        public static char GetCIdentifierChar(Random random)
        {
            int i = random.Next(0, 63);

            if (i < 10)
            {
                return (char)(NumberBase + i);
            }

            i = i - 10;

            if (i < 26)
            {
                return (char)(UpperCaseLetterBase + i);
            }

            i = i - 26;

            if (i == 0)
            {
                return (char)(Underline);
            }

            i--;

            return (char)(LowerCaseLetterBase + i);
        }

        public static char GetNormalChar(Random random)
        {
            return (char)random.Next(0x20, 0x7E);
        }

        public static string NextString(Random Randomint, int length, char[] ValidChars)
        {
            return new String(
                Enumerable.Repeat(0, length)
                    .Select(p => GetRandomItem(Randomint, ValidChars))
                    .ToArray());
        }

        public static string NextNonASCIIString(Random Randomint)
        {
            var builder = new StringBuilder(FileOp.NextString(Randomint));
            var countToInsert = Randomint.Next(1, 50);
            for (int i = 0; i < countToInsert; i++)
            {
                char ch;
                while (true)
                {
                    ch = FileOp.GetRandomVisiableChar(Randomint);
                    if ((int)ch >= 0x80)
                    {
                        break;
                    }
                }

                builder.Insert(Randomint.Next(0, builder.Length + 1), ch);
            }

            return builder.ToString();
        }

        public static T GetRandomItem<T>(Random Randomint, T[] items)
        {
            if (items.Length <= 0)
            {
                Test.Error("no candidate item");
            }

            int i = Randomint.Next(0, items.Length);
            return items[i];
        }

        public static char GetRandomVisiableChar(Random Randomint)
        {
            double specialCharProbability = 0.05;

            if (Randomint.Next(0, 100) / 100.0 < specialCharProbability)
            {
                return ValidSpecialCharsInLocalFileName[Randomint.Next(0, ValidSpecialCharsInLocalFileName.Count)];
            }
            else
            {
                while (true)
                {
                    int i = Randomint.Next(0x20, 0xD7FF);
                    var ch = (char)i;

                    // Control characters are all invalid to blob name.
                    // Characters U+200E, U+200F, U+202A, U+202B, U+202C, U+202D, U+202E are all invalid to URI.
#if DNXCORE50
                    if (CharUnicodeInfo.GetUnicodeCategory(ch) != UnicodeCategory.Control &&
#else
                    if (char.GetUnicodeCategory(ch) != UnicodeCategory.Control &&
#endif
                        !InvalidCharsInLocalAndCloudFileName.Contains(ch) &&
                        i != 0x200e && i != 0x200f &&
                        i != 0x202a && i != 0x202b && i != 0x202c && i != 0x202d && i != 0x202e)
                    {
                        return ch;
                    }
                }
            }
        }

        public static string GetDriveMapping(char letter)
        {
            var sb = new StringBuilder(259);
            if (QueryDosDevice(CreateDeviceName(letter), sb, sb.Capacity) == 0)
            {
                // Return empty string if the drive is not mapped
                int err = Marshal.GetLastWin32Error();
                if (err == 2) return "";
                throw new System.ComponentModel.Win32Exception();
            }
            return sb.ToString().Substring(4);
        }

        private static string CreateDeviceName(char letter)
        {
            return new string(char.ToUpper(letter), 1) + ":";
        }

#if DNXCORE50
        [DllImport("kernel32.dll", CharSet = CharSet.Ansi, SetLastError = true)]
#else
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
#endif
        private static extern bool DefineDosDevice(int flags, string devname, string path);
#if DNXCORE50
        [DllImport("kernel32.dll", CharSet = CharSet.Ansi, SetLastError = true)]
#else
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
#endif
        private static extern int QueryDosDevice(string devname, StringBuilder buffer, int bufSize);

        public static void SetFileAttribute(string Filename, FileAttributes attribute)
        {
            FileAttributes fa = File.GetAttributes(Filename);
            if ((fa & attribute) == attribute)
            {
                Test.Info("Attribute {0} is already in file {1}. Don't need to add again.", attribute.ToString(), Filename);
                return;
            }

            switch (attribute)
            {
#if !DNXCORE50
                case FileAttributes.Encrypted:
                    string fullPath = GetFullPath(Filename);
                    File.Encrypt(fullPath);
                    break;
#endif
                case FileAttributes.Normal:
                    RemoveFileAttribute(Filename, FileAttributes.Encrypted);
                    RemoveFileAttribute(Filename, FileAttributes.Compressed);
                    fa = FileAttributes.Normal;
                    File.SetAttributes(Filename, fa);
                    break;
                case FileAttributes.Compressed:
                    compress(Filename);
                    break;
                default:
                    fa = fa | attribute;
                    File.SetAttributes(Filename, fa);
                    break;
            }
            Test.Info("Attribute {0} is added to file {1}.", attribute.ToString(), Filename);
        }

        private static string GetFullPath(string Filename)
        {
            string fullPath = Path.GetFullPath(Filename);
            char driveLetter = fullPath.ToCharArray()[0];
            String actualPath = GetDriveMapping(driveLetter);
            // WAES will map c:\user\tasks\workitems\{jobid} to f:\wd, 
            // and File.Encrypt will throw DirectoryNotFoundException
            // Thus it is necessary to convert the file path to original one
            if (Regex.IsMatch(actualPath, @"\w:\\", RegexOptions.IgnoreCase) == true)
            {
                fullPath = String.Format("{0}{1}", actualPath, fullPath.Substring(2));
            }
            return fullPath;
        }

        public static void RemoveFileAttribute(string Filename, FileAttributes attribute)
        {
            FileAttributes fa = File.GetAttributes(Filename);
            if ((fa & attribute) != attribute)
            {
                Test.Info("Attribute {0} is NOT in file{1}. Don't need to remove.", attribute.ToString(), Filename);
                return;
            }

            switch (attribute)
            {
#if !DNXCORE50
                case FileAttributes.Encrypted:
                    File.Decrypt(GetFullPath(Filename));
                    break;
#endif
                case FileAttributes.Normal:
                    fa = fa | FileAttributes.Archive;
                    File.SetAttributes(Filename, fa);
                    break;
                case FileAttributes.Compressed:
                    uncompress(Filename);
                    break;
                default:
                    fa = fa & ~attribute;
                    File.SetAttributes(Filename, fa);
                    break;
            }
            Test.Info("Attribute {0} is removed from file{1}.", attribute.ToString(), Filename);
        }

        [DllImport("kernel32.dll")]
        public static extern int DeviceIoControl(SafeHandle hDevice, int
        dwIoControlCode, ref short lpInBuffer, int nInBufferSize, IntPtr
        lpOutBuffer, int nOutBufferSize, ref int lpBytesReturned, IntPtr
        lpOverlapped);

#if !DNXCORE50
        private static int FSCTL_SET_COMPRESSION = 0x9C040;
        private static short COMPRESSION_FORMAT_DEFAULT = 1;
        private static short COMPRESSION_FORMAT_NONE = 0;
#endif

#pragma warning disable 612, 618
        public static void compress(string filename)
        {
#if !DNXCORE50
            if ((File.GetAttributes(filename) & FileAttributes.Encrypted) == FileAttributes.Encrypted)
            {
                Test.Info("Decrypt File {0} to prepare for compress.", filename);
                File.Decrypt(GetFullPath(filename));
            }
            int lpBytesReturned = 0;
            FileStream f = LongPathFileExtension.Open(filename, System.IO.FileMode.Open,
            System.IO.FileAccess.ReadWrite, System.IO.FileShare.None);
            int result = DeviceIoControl(f.SafeFileHandle, FSCTL_SET_COMPRESSION,
            ref COMPRESSION_FORMAT_DEFAULT, 2 /*sizeof(short)*/, IntPtr.Zero, 0,
            ref lpBytesReturned, IntPtr.Zero);
            f.Close();
#endif
        }

        public static void uncompress(string filename)
        {
#if !DNXCORE50
            int lpBytesReturned = 0;
            FileStream f = LongPathFileExtension.Open(filename, System.IO.FileMode.Open,
            System.IO.FileAccess.ReadWrite, System.IO.FileShare.None);
            int result = DeviceIoControl(f.SafeFileHandle, FSCTL_SET_COMPRESSION,
            ref COMPRESSION_FORMAT_NONE, 2 /*sizeof(short)*/, IntPtr.Zero, 0,
            ref lpBytesReturned, IntPtr.Zero);
            f.Close();
#endif
        }
#pragma warning restore 612, 618

    }

    public class CloudFileHelper
    {
        public const string AllowedCharactersInShareName = "abcdefghijklmnopqrstuvwxyz0123456789-";
        public const string InvalidCharactersInDirOrFileName = "\"\\/:|<>*?";
        public const int MinShareNameLength = 3;
        public const int MaxShareNameLength = 63;
        public const int MinDirOrFileNameLength = 1;
        public const int MaxDirOrFileNameLength = 255;

        public CloudStorageAccount Account
        {
            get;
            private set;
        }

        public CloudFileClient FileClient
        {
            get;
            private set;
        }

        public CloudFileHelper(CloudStorageAccount account)
        {
            this.Account = account;
            this.FileClient = account.CreateCloudFileClient();
            this.FileClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.Zero, 3);
        }

        public bool Exists(string shareName)
        {
            CloudFileShare share = this.FileClient.GetShareReference(shareName);
            return share.Exists();
        }

        public bool CreateShare(string shareName)
        {
            CloudFileShare share = this.FileClient.GetShareReference(shareName);
            return share.CreateIfNotExists();
        }

        public bool CleanupShare(string shareName)
        {
            return this.CleanupFileDirectory(shareName, string.Empty);
        }

        public bool CleanupShareByRecreateIt(string shareName)
        {
            try
            {
                CloudFileShare share = FileClient.GetShareReference(shareName);
                if (share == null || !share.Exists()) return false;

                FileRequestOptions fro = new FileRequestOptions();
                fro.RetryPolicy = new LinearRetry(new TimeSpan(0, 1, 0), 3);

                share.DeleteIfExists(null, fro);

                Test.Info("Share deleted.");
                fro.RetryPolicy = new LinearRetry(new TimeSpan(0, 3, 0), 3);

                bool createSuccess = false;
                int retry = 0;
                while (!createSuccess && retry++ < 100) //wait up to 5 minutes
                {
                    try
                    {
                        share.Create(fro);
                        createSuccess = true;
                        Test.Info("Share recreated.");
                    }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                    catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
#else
                    catch (StorageException e)
#endif
                    {
                        if (e.Message.Contains("(409)")) //conflict, the container is still in deleteing
                        {
                            Thread.Sleep(3000);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }

                return createSuccess;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public bool DeleteShare(string shareName)
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CloudFileShare share = this.FileClient.GetShareReference(shareName);
            return share.DeleteIfExistsAsync(DeleteShareSnapshotsOption.IncludeSnapshots, null, null, null, tokenSource.Token).Result;
        }

        public bool DownloadFile(string shareName, string fileName, string filePath)
        {
            try
            {
                CloudFileShare share = FileClient.GetShareReference(shareName);
                FileRequestOptions fro = new FileRequestOptions();
                fro.RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 30), 3);
                fro.ServerTimeout = new TimeSpan(1, 30, 0);
                fro.MaximumExecutionTime = new TimeSpan(1, 30, 0);

                CloudFileDirectory root = share.GetRootDirectoryReference();
                CloudFile cloudFile = root.GetFileReference(fileName);

                using (FileStream fs = LongPathFileExtension.Open(filePath, FileMode.Create))
                {
                    cloudFile.DownloadToStream(fs, null, fro);
#if DNXCORE50
                    fs.Dispose();
#else
                    fs.Close();
#endif
                }

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public bool UploadFile(string shareName, string fileName, string filePath, bool createParentIfNotExist = true)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            try
            {
                FileRequestOptions options = new FileRequestOptions
                {
                    RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(60), 3),
                };

                if (createParentIfNotExist)
                {
                    share.CreateIfNotExists(options);
                    string parentDirectoryPath = GetFileDirectoryName(fileName);
                    this.CreateFileDirectory(shareName, parentDirectoryPath);
                }
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
            catch (StorageException e)
            {
#endif
                Test.Error("UploadFile: receives StorageException when creating parent: {0}", e.ToString());
                return false;
            }
            catch (Exception e)
            {
                Test.Error("UploadFile: receives Exception when creating parent: {0}", e.ToString());
                return false;
            }

            CloudFileDirectory root = share.GetRootDirectoryReference();
            CloudFile cloudFile = root.GetFileReference(fileName);
            return UploadFile(cloudFile, filePath);
        }

        public static bool UploadFile(CloudFile destFile, string sourceFile)
        {
            try
            {
                FileInfo fi = new FileInfo(sourceFile);
                if (!fi.Exists)
                {
                    return false;
                }

                FileRequestOptions fro = new FileRequestOptions();
                fro.RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 60), 5);
                fro.ServerTimeout = new TimeSpan(1, 90, 0);
                fro.MaximumExecutionTime = new TimeSpan(1, 90, 0);

                destFile.Create(fi.Length, null, fro);

                using (FileStream fs = LongPathFileExtension.Open(sourceFile, FileMode.Open))
                {
                    destFile.UploadFromStream(fs, null, fro);
#if DNXCORE50
                    fs.Dispose();
#else
                    fs.Close();
#endif
                }

                // update content md5
                destFile.Properties.ContentMD5 = Helper.GetFileContentMD5(sourceFile);
                destFile.SetProperties(null, fro);

                Test.Info("Local file {0} has been uploaded to xSMB successfully", sourceFile);

                return true;
            }
            catch (Exception e) when (e is StorageException || e.InnerException is StorageException)
            {
                Test.Error("UploadFile: receives StorageException: {0}", e.ToString());
                return false;
            }
            catch (Exception e)
            {
                Test.Error("UploadFile: receives Exception: {0}", e.ToString());
                return false;
            }
        }

        public static void GenerateCloudFileWithRangedData(CloudFile cloudFile, List<int> ranges, List<int> gaps)
        {
            Helper.GenerateSparseCloudObject(
                ranges,
                gaps,
                createObject: (totalSize) =>
                {
                    cloudFile.Create(totalSize);
                },
                writeUnit: (unitOffset, randomData) =>
                {
                    cloudFile.WriteRange(randomData, unitOffset, options: HelperConst.DefaultFileOptions);
                });

            Helper.PrintCloudFileRanges(cloudFile, true);

            // Set correct MD5 to cloud file
            string md5 = CalculateMD5ByDownloading(cloudFile);
            cloudFile.Properties.ContentMD5 = md5;
            cloudFile.SetProperties(options: HelperConst.DefaultFileOptions);
        }

        public CloudFile QueryFile(string shareName, string fileName)
        {
            try
            {
                CloudFileShare share = this.FileClient.GetShareReference(shareName);
                CloudFileDirectory root = share.GetRootDirectoryReference();
                CloudFile file = root.GetFileReference(fileName);

                if (file.Exists())
                {
                    file.FetchAttributes();
                    return file;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        public bool DeleteFile(string shareName, string fileName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            if (share.Exists())
            {
                CloudFileDirectory root = share.GetRootDirectoryReference();
                CloudFile file = root.GetFileReference(fileName);

                return file.DeleteIfExists();
            }

            return false;
        }

        public bool DeleteFileDirectory(string shareName, string fileDirectoryName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            if (!share.Exists())
            {
                return false;
            }

            // do not try to delete a root directory
            if (string.IsNullOrEmpty(fileDirectoryName))
            {
                return false;
            }

            CloudFileDirectory root = share.GetRootDirectoryReference();
            CloudFileDirectory dir = root.GetDirectoryReference(fileDirectoryName);

            if (!dir.Exists())
            {
                return false;
            }

            DeleteFileDirectory(dir);

            return true;
        }

        public CloudFileDirectory QueryFileDirectory(string shareName, string fileDirectoryName)
        {
            try
            {
                CloudFileShare share = FileClient.GetShareReference(shareName);
                if (share == null || !share.Exists()) return null;

                CloudFileDirectory root = share.GetRootDirectoryReference();
                if (string.IsNullOrEmpty(fileDirectoryName))
                {
                    return root;
                }

                CloudFileDirectory dir = root.GetDirectoryReference(fileDirectoryName);

                return dir.Exists() ? dir : null;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        public bool CreateFileDirectory(string shareName, string fileDirectoryName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            if (share == null || !share.Exists())
            {
                return false;
            }

            if (string.IsNullOrEmpty(fileDirectoryName))
            {
                return false;
            }

            CloudFileDirectory parent = share.GetRootDirectoryReference();

            string[] directoryTokens = fileDirectoryName.Split('/');
            foreach (string directoryToken in directoryTokens)
            {
                parent = CreateFileDirectoryIfNotExist(parent, directoryToken);
            }

            return true;
        }

        // create a directory under the specified parent LongPathDirectoryExtention.
        public static CloudFileDirectory CreateFileDirectoryIfNotExist(CloudFileDirectory parent, string fileDirectoryName)
        {
            CloudFileDirectory dir = parent.GetDirectoryReference(fileDirectoryName);
            dir.CreateIfNotExists();

            return dir;
        }

        // upload all files & dirs(including empty dir) in a local directory to an xsmb directory
        public void UploadDirectory(string localDirName, string shareName, string fileDirName, bool recursive)
        {
            DirectoryInfo srcDir = new DirectoryInfo(localDirName);
            CloudFileDirectory destDir = QueryFileDirectory(shareName, fileDirName);
            if (null == destDir)
            {
                this.CreateFileDirectory(shareName, fileDirName);
                destDir = QueryFileDirectory(shareName, fileDirName);
                Test.Assert(null != destDir, "{0} should exist in file share {1}.", fileDirName, shareName);
            }

            UploadDirectory(srcDir, destDir, recursive);
        }

        public static void UploadDirectory(DirectoryInfo sourceDir, CloudFileDirectory destDir, bool recursive)
        {
            destDir.CreateIfNotExists();

            Parallel.ForEach(
                sourceDir.EnumerateFiles(),
                fi =>
                {
                    string fileName = Path.GetFileName(fi.Name);
                    CloudFile file = destDir.GetFileReference(fileName);

                    bool uploaded = UploadFile(file, fi.FullName);
                    if (!uploaded)
                    {
                        Test.Assert(false, "failed to upload file:{0}", fi.FullName);
                    }
                });

            if (recursive)
            {
                foreach (DirectoryInfo di in sourceDir.EnumerateDirectories())
                {
                    string subDirName = Path.GetFileName(di.Name);
                    CloudFileDirectory subDir = destDir.GetDirectoryReference(subDirName);
                    UploadDirectory(di, subDir, true);
                }
            }
        }

        // compare an xsmb directory with a local LongPathDirectoryExtention. return true only if
        // 1. all files under both dir are the same, and
        // 2. all sub directories under both dir are the same
        public bool CompareCloudFileDirAndLocalDir(string shareName, string fileDirName, string localDirName)
        {
            try
            {
                CloudFileDirectory dir = QueryFileDirectory(shareName, fileDirName);
                if (null == dir)
                {
                    return false;
                }

                return CompareCloudFileDirAndLocalDir(dir, localDirName);
            }
            catch
            {
                return false;
            }
        }

        public static bool CompareCloudFileDirAndLocalDir(CloudFileDirectory dir, string localDirName)
        {
            if (!dir.Exists() || !LongPathDirectoryExtension.Exists(localDirName))
            {
                // return false if cloud dir or local dir not exist.
                Test.Info("dir not exist. local dir={0}", localDirName);
                return false;
            }

            HashSet<string> localSubFiles = new HashSet<string>();
            foreach (string localSubFile in Directory.EnumerateFiles(localDirName))
            {
                localSubFiles.Add(Path.GetFileName(localSubFile));
            }

            HashSet<string> localSubDirs = new HashSet<string>();
            foreach (string localSubDir in Directory.EnumerateDirectories(localDirName))
            {
                localSubDirs.Add(Path.GetFileName(localSubDir));
            }

            foreach (IListFileItem item in dir.ListFilesAndDirectories(HelperConst.DefaultFileOptions))
            {
                if (item is CloudFile)
                {
                    CloudFile tmpFile = item as CloudFile;

                    // TODO: tmpFile.RelativeName
                    string tmpFileName = Path.GetFileName(tmpFile.Name);
                    if (!localSubFiles.Remove(tmpFileName))
                    {
                        Test.Info("file not found at local: {0}", tmpFile.Name);
                        return false;
                    }

                    if (!CompareCloudFileAndLocalFile(tmpFile, Path.Combine(localDirName, tmpFileName)))
                    {
                        Test.Info("file content not consistent: {0}", tmpFile.Name);
                        return false;
                    }
                }
                else if (item is CloudFileDirectory)
                {
                    CloudFileDirectory tmpDir = item as CloudFileDirectory;
                    string tmpDirName = tmpDir.Name;
                    if (!localSubDirs.Remove(tmpDirName))
                    {
                        Test.Info("dir not found at local: {0}", tmpDir.Name);
                        return false;
                    }

                    if (!CompareCloudFileDirAndLocalDir(tmpDir, Path.Combine(localDirName, tmpDirName)))
                    {
                        return false;
                    }
                }
            }

            return (localSubFiles.Count == 0 && localSubDirs.Count == 0);
        }

        public bool CompareCloudFileAndLocalFile(string shareName, string fileName, string localFileName)
        {
            CloudFile file = QueryFile(shareName, fileName);
            if (null == file)
            {
                return false;
            }

            return CompareCloudFileAndLocalFile(file, localFileName);
        }

        public static bool CompareCloudFileAndLocalFile(CloudFile file, string localFileName)
        {
            if (!file.Exists() || !File.Exists(localFileName))
            {
                return false;
            }

            file.FetchAttributes();
            return file.Properties.ContentMD5 == Helper.GetFileContentMD5(localFileName);
        }

        public static string CalculateMD5ByDownloading(CloudFile cloudFile, bool disableMD5Check = false)
        {
#if DNXCORE50
            const int bufferSize = 4 * 1024 * 1024;
            cloudFile.FetchAttributes();
            long blobSize = cloudFile.Properties.Length;
            byte[] buffer = new byte[bufferSize];


            long index = 0;
            using (IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.MD5))
            {
                do
                {
                    long sizeToRead = blobSize - index < bufferSize ? blobSize - index : bufferSize;

                    if (sizeToRead <= 0)
                    {
                        break;
                    }

                    cloudFile.DownloadRangeToByteArrayAsync(buffer, 0, index, sizeToRead).Wait();
                    index += sizeToRead;
                    hash.AppendData(buffer, 0, (int)sizeToRead);
                }
                while (true);

                return Convert.ToBase64String(hash.GetHashAndReset());
            }
#else
            using (TemporaryTestFolder tempFolder = new TemporaryTestFolder(Guid.NewGuid().ToString()))
            {
                const string tempFileName = "tempFile";
                string tempFilePath = Path.Combine(tempFolder.Path, tempFileName);
                var fileOptions = new FileRequestOptions();
                fileOptions.DisableContentMD5Validation = disableMD5Check;
                fileOptions.RetryPolicy = HelperConst.DefaultFileOptions.RetryPolicy.CreateInstance();
                cloudFile.DownloadToFile(tempFilePath, FileMode.OpenOrCreate, options: fileOptions);
                return Helper.GetFileContentMD5(tempFilePath);
            }
#endif
        }

        public CloudFile GetFileReference(string shareName, string cloudFileName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            CloudFileDirectory dir = share.GetRootDirectoryReference();
            return dir.GetFileReference(cloudFileName);
        }

        public CloudFileDirectory GetDirReference(string shareName, string cloudDirName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            CloudFileDirectory dir = share.GetRootDirectoryReference();

            if (cloudDirName == string.Empty)
            {
                return dir;
            }
            else
            {
                return dir.GetDirectoryReference(cloudDirName);
            }
        }

        // enumerate files under the specified cloud directory.
        // Returns an enumerable collection of the full names(including dirName), for the files in the directory.
        public IEnumerable<string> EnumerateFiles(string shareName, string dirName, bool recursive)
        {
            CloudFileDirectory dir = QueryFileDirectory(shareName, dirName);
            if (null == dir)
            {
                Test.Assert(false, "directory or share doesn't exist");
            }

            return EnumerateFiles(dir, recursive);
        }

        // enumerate files under the specified cloud directory.
        // Returns an enumerable collection of the full names(including dir name), for the files in the directory.
        public static IEnumerable<string> EnumerateFiles(CloudFileDirectory dir, bool recursive)
        {
            var folders = new List<CloudFileDirectory>();
            foreach (IListFileItem item in dir.ListFilesAndDirectories(HelperConst.DefaultFileOptions))
            {
                if (item is CloudFile)
                {
                    CloudFile file = item as CloudFile;
                    string fileName = Path.GetFileName(file.Name);
                    string filePath = dir.Name + "/" + fileName;
                    yield return filePath;
                }
                else if (item is CloudFileDirectory)
                {
                    if (recursive)
                    {
                        CloudFileDirectory subDir = item as CloudFileDirectory;
                        folders.Add(subDir);
                    }
                }
            }

            foreach (var folder in folders)
            {
                foreach (var filePath in EnumerateFiles(folder, recursive))
                {
                    yield return dir.Name + "/" + filePath;
                }
            }
        }

        // enumerate directory under the specified cloud directory.
        // Returns an enumerable collection of the full names(including dirName), for the directories in the directory
        public IEnumerable<string> EnumerateDirectories(string shareName, string dirName, bool recursive)
        {
            CloudFileDirectory dir = QueryFileDirectory(shareName, dirName);
            if (null == dir)
            {
                Test.Assert(false, "directory or share doesn't exist");
            }

            return EnumerateDirectories(dir, recursive);
        }

        // enumerate directory under the specified cloud directory.
        // Returns an enumerable collection of the full names(including dir name), for the directories in the directory
        public static IEnumerable<string> EnumerateDirectories(CloudFileDirectory dir, bool recursive)
        {
            List<string> dirs = new List<string>();
            foreach (IListFileItem item in dir.ListFilesAndDirectories(HelperConst.DefaultFileOptions))
            {
                if (item is CloudFileDirectory)
                {
                    CloudFileDirectory subDir = item as CloudFileDirectory;
                    dirs.Add(dir.Name + "/" + subDir.Name);

                    if (recursive)
                    {
                        foreach (string subSubDir in EnumerateDirectories(subDir, true))
                        {
                            dirs.Add(dir.Name + "/" + subSubDir);
                        }
                    }
                }
            }

            return dirs;
        }

        // convert xsmb file name to local file name by replacing "/" with DirectorySeparatorChar
        public static string ConvertCloudFileNameToLocalFileName(string fileName)
        {
            if (Path.DirectorySeparatorChar == '/')
            {
                return fileName;
            }

            return fileName.Replace('/', Path.DirectorySeparatorChar);
        }

        // convert local file name to xsmb  by replacing  DirectorySeparatorChar with "/"
        public static string ConvertLocalFileNameToCloudFileName(string fileName)
        {
            if (Path.DirectorySeparatorChar == '/')
            {
                return fileName;
            }

            return fileName.Replace(Path.DirectorySeparatorChar, '/');
        }

        public static string GetFileDirectoryName(string fileName)
        {
            int index = fileName.LastIndexOf('/');

            if (-1 == index)
            {
                return string.Empty;
            }

            return fileName.Substring(0, index);
        }

        public bool CleanupFileDirectory(string shareName, string fileDirectoryName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            if (!share.Exists())
            {
                return false;
            }

            CloudFileDirectory root = share.GetRootDirectoryReference();
            if (!string.IsNullOrEmpty(fileDirectoryName))
            {
                root = root.GetDirectoryReference(fileDirectoryName);
            }
            else
            {
                if (root.ListFilesAndDirectories(HelperConst.DefaultFileOptions).Count() > 500)
                    return CleanupFileShareByRecreateIt(shareName);
            }

            CleanupFileDirectory(root);
            return true;
        }

        public static void CleanupFileDirectory(CloudFileDirectory cloudDirectory)
        {
            foreach (IListFileItem item in cloudDirectory.ListFilesAndDirectories(HelperConst.DefaultFileOptions))
            {
                if (item is CloudFile)
                {
                    var file = (item as CloudFile);
                    try
                    {
                        file.Delete();
                    }
                    catch (StorageException se)
                    {
                        if (Helper.IsConflictStorageException(se))
                        {
                            file.Properties.NtfsAttributes = CloudFileNtfsAttributes.Normal;
                            file.SetProperties(null, HelperConst.DefaultFileOptions, null);
                            file.Delete(options: HelperConst.DefaultFileOptions);
                        }
                    }
                }

                if (item is CloudFileDirectory)
                {
                    DeleteFileDirectory(item as CloudFileDirectory);
                }
            }
        }

        public bool CleanupFileShareByRecreateIt(string shareName)
        {
            CloudFileShare share = FileClient.GetShareReference(shareName);
            if (!share.Exists())
            {
                return true;
            }

            try
            {
                share.Delete();

                Test.Info("share deleted.");

                bool createSuccess = false;
                int retry = 0;
                while (!createSuccess && retry++ < 100) //wait up to 5 minutes
                {
                    try
                    {
                        share.Create();
                        createSuccess = true;
                        Test.Info("share recreated.");
                    }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                    catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
#else
                    catch (StorageException e)
#endif
                    {
                        if (e.Message.Contains("(409)")) //conflict, the share is still in deleteing
                        {
                            Thread.Sleep(3000);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public static void DeleteFileDirectory(CloudFileDirectory cloudDirectory)
        {
            CleanupFileDirectory(cloudDirectory);
            try
            {
                cloudDirectory.Delete();
            }
            catch (StorageException)
            {
                cloudDirectory.Properties.NtfsAttributes = CloudFileNtfsAttributes.Normal;
                cloudDirectory.SetProperties(HelperConst.DefaultFileOptions);
                cloudDirectory.Delete();
            }
        }

        /// <summary>
        /// Get SAS of a share with specific permission and period.
        /// </summary>
        /// <param name="shareName">The name of the share.</param>
        /// <param name="sap">The permission of the SAS.</param>
        /// <param name="validatePeriod">How long the SAS will be valid before expire, in second</param>
        /// <returns>the SAS</returns>
        public string GetSASofShare(
            string shareName,
            SharedAccessFilePermissions permissions,
            int validatePeriod,
            bool UseSavedPolicy = true,
            string policySignedIdentifier = "PolicyIdentifier")
        {
            var share = this.FileClient.GetShareReference(shareName);
            string sas = string.Empty;
            var policy = new SharedAccessFilePolicy();
            policy.Permissions = permissions;
            policy.SharedAccessExpiryTime = DateTimeOffset.Now.AddSeconds(validatePeriod);
            if (UseSavedPolicy)
            {
                var sharePermissions = share.GetPermissions();
                sharePermissions.SharedAccessPolicies.Clear();
                sharePermissions.SharedAccessPolicies.Add(policySignedIdentifier, policy);
                share.SetPermissions(sharePermissions);
                sas = share.GetSharedAccessSignature(new SharedAccessFilePolicy(), policySignedIdentifier);

                DMLibTestHelper.WaitForACLTakeEffect();
            }
            else
            {
                sas = share.GetSharedAccessSignature(policy);
            }

            Test.Info("The SAS is {0}", sas);
            return sas;
        }

        /// <summary>
        /// Clears the SAS policy set to a container, used to revoke the SAS.
        /// </summary>
        /// <param name="shareName">The name of the share.</param>
        public void ClearSASPolicyofShare(string shareName)
        {
            var share = this.FileClient.GetShareReference(shareName);
            var bp = share.GetPermissions();
            bp.SharedAccessPolicies.Clear();
            share.SetPermissions(bp);
        }
    }

    /// <summary>
    /// This class helps to do operations on cloud blobs
    /// </summary>
    public class CloudBlobHelper
    {
        public const string RootContainer = "$root";

        private CloudStorageAccount account;

        /// <summary>
        /// The storage account
        /// </summary>
        public CloudStorageAccount Account
        {
            get { return account; }
            private set { account = value; }
        }

        private CloudBlobClient blobClient;
        /// <summary>
        /// The blob client
        /// </summary>
        public CloudBlobClient BlobClient
        {
            get { return blobClient; }
            set { blobClient = value; }
        }

        /// <summary>
        /// Construct the helper with the storage account
        /// </summary>
        /// <param name="account"></param>
        public CloudBlobHelper(CloudStorageAccount account)
        {
            Account = account;
            BlobClient = account.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.Zero, 3);
        }

        /// <summary>
        /// Construct the helper with the storage account
        /// </summary>
        /// <param name="account"></param>
        public CloudBlobHelper(string ConnectionString)
        {
            Account = CloudStorageAccount.Parse(ConnectionString);
            BlobClient = Account.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.Zero, 3);
        }

        public bool Exists(string containerName)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            return container.Exists();
        }

        /// <summary>
        /// Create a container for blobs
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <returns>Return true on success, false if already exists, throw exception on error</returns>
        public bool CreateContainer(string containerName)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            return container.CreateIfNotExists();
        }

        /// <summary>
        /// Delete the container for the blobs
        /// </summary>
        /// <param name="containerName">the name of container</param>
        /// <returns>Return true on success (or the container was deleted before), false if the container doesnot exist, throw exception on error</returns>
        public bool DeleteContainer(string containerName)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            return container.DeleteIfExists();
        }

        public CloudBlobContainer GetGRSContainer(string containerName)
        {
            return new CloudBlobContainer(
                        new Uri(string.Format("{0}/{1}", this.Account.BlobStorageUri.SecondaryUri.AbsoluteUri, containerName)),
                        this.Account.Credentials);
        }

        public BlobContainerPermissions SetGRSContainerAccessType(string containerName, BlobContainerPublicAccessType accessType)
        {
            BlobContainerPermissions oldPermissions = this.SetContainerAccessType(containerName, accessType);
            if (null == oldPermissions)
            {
                return null;
            }

            CloudBlobContainer containerGRS = new CloudBlobContainer(
                    new Uri(string.Format("{0}/{1}", this.Account.BlobStorageUri.SecondaryUri.AbsoluteUri, containerName)),
                    this.blobClient.Credentials);

            Helper.WaitForTakingEffect(containerGRS.ServiceClient);
            return oldPermissions;
        }

        /// <summary>
        /// Set the specific container to the accesstype
        /// </summary>
        /// <param name="containerName">container Name</param>
        /// <param name="accesstype">the accesstype the contain will be set</param>
        /// <returns>the container 's permission before set, so can be set back when test case finish</returns>
        public BlobContainerPermissions SetContainerAccessType(string containerName, BlobContainerPublicAccessType accesstype)
        {
            try
            {
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                container.CreateIfNotExists();
                BlobContainerPermissions oldPerm = container.GetPermissions();
                BlobContainerPermissions blobPermissions = new BlobContainerPermissions();
                blobPermissions.PublicAccess = accesstype;
                container.SetPermissions(blobPermissions);
                return oldPerm;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        public bool ListBlobs(string containerName, out List<CloudBlob> blobList)
        {
            return this.ListBlobs(containerName, BlobListingDetails.All, out blobList);
        }


        public bool ListBlobs(string containerName, BlobListingDetails listingDetails, out List<CloudBlob> blobList)
        {
            blobList = new List<CloudBlob>();
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                IEnumerable<IListBlobItem> blobs = container.ListBlobs(null, true, listingDetails, HelperConst.DefaultBlobOptions);
                if (blobs != null)
                {
                    foreach (CloudBlob blob in blobs)
                    {
                        blobList.Add(blob);
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }


        /// <summary>
        /// list blobs in a folder, TODO: implement this for batch operations on blobs
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobList"></param>
        /// <returns></returns>
        public bool ListBlobs(string containerName, string folderName, out List<CloudBlob> blobList)
        {
            blobList = new List<CloudBlob>();

            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlobDirectory blobDir = container.GetDirectoryReference(folderName);
                IEnumerable<IListBlobItem> blobs = blobDir.ListBlobs(true, BlobListingDetails.All);
                if (blobs != null)
                {
                    foreach (CloudBlob blob in blobs)
                    {
                        blobList.Add(blob);
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        /// <summary>
        /// Validate the uploaded tree which is created by Helper.GenerateFixedTestTree()
        /// </summary>
        /// <param name="filename">the file prefix of the tree</param>
        /// <param name="foldername">the folder prefix of the tree</param>
        /// <param name="sourceFolder"></param>
        /// <param name="destFolder"></param>
        /// <param name="size">how many files in each folder</param>
        /// <param name="layer">how many folder level to verify</param>
        /// <param name="containerName">the container which contain the uploaded tree</param>
        /// <param name="empty">true means should verify the folder not exist. false means verify the folder exist.</param>
        /// <returns>true if verify pass, false mean verify fail</returns>
        public bool ValidateFixedTestTree(string filename, string foldername, string sourceFolder, string destFolder, int size, int layer, string containerName, bool empty = false)
        {
            Test.Info("Verify the folder {0}...", sourceFolder);
            for (int i = 0; i < size; i++)
            {
                string sourcefilename = sourceFolder + Path.DirectorySeparatorChar + filename + "_" + i;
                string destblobname = destFolder + Path.DirectorySeparatorChar + filename + "_" + i;
                CloudBlob blob = this.QueryBlob(containerName, destblobname);
                if (!empty)
                {
                    if (blob == null)
                    {
                        Test.Error("Blob {0} not exist.", destblobname);
                        return false;
                    }
                    string source_MD5 = Helper.GetFileContentMD5(sourcefilename);
                    string Dest_MD5 = blob.Properties.ContentMD5;
                    if (source_MD5 != Dest_MD5)
                    {
                        Test.Error("sourcefile:{0}: {1} == destblob:{2}:{3}", sourcefilename, source_MD5, destblobname, Dest_MD5);
                        return false;
                    }
                }
                else
                {
                    if (blob != null && blob.Properties.Length != 0)
                    {
                        Test.Error("Blob {0} should not exist.", destblobname);
                        return false;
                    }
                }
            }
            if (layer > 0)
            {
                for (int i = 0; i < size; i++)
                {
                    if (!ValidateFixedTestTree(filename, foldername, sourceFolder + Path.DirectorySeparatorChar + foldername + "_" + i, destFolder + Path.DirectorySeparatorChar + foldername + "_" + i, size, layer - 1, containerName, empty))
                        return false;
                }

            }

            return true;
        }

        /// <summary>
        /// Validate the uploaded tree which is created by Helper.GenerateFixedTestTree()
        /// </summary>
        /// <param name="filename">the file prefix of the tree</param>
        /// <param name="foldername">the folder prefix of the tree</param>
        /// <param name="currentFolder">current folder to validate</param>
        /// <param name="size">how many files in each folder</param>
        /// <param name="layer">how many folder level to verify</param>
        /// <param name="containerName">the container which contain the uploaded tree</param>
        /// <param name="empty">true means should verify the folder not exist. false means verify the folder exist.</param>
        /// <returns>true if verify pass, false mean verify fail</returns>
        public bool ValidateFixedTestTree(string filename, string foldername, string currentFolder, int size, int layer, string containerName, bool empty = false)
        {
            Test.Info("Verify the folder {0}...", currentFolder);
            return this.ValidateFixedTestTree(filename, foldername, currentFolder, currentFolder, size, layer, containerName, empty);
        }

        /// <summary>
        /// Get SAS of a container with specific permission and period
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="sap">the permission of the SAS</param>
        /// <param name="validatePeriod">How long the SAS will be valid before expire, in second</param>
        /// <returns>the SAS</returns>
        public string GetSASofContainer(string containerName, SharedAccessBlobPermissions SAB, int validatePeriod, bool UseSavedPolicy = true, string PolicySignedIdentifier = "PolicyIdentifier")
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                string SAS = string.Empty;
                SharedAccessBlobPolicy sap = new SharedAccessBlobPolicy();
                sap.Permissions = SAB;
                sap.SharedAccessExpiryTime = DateTimeOffset.Now.AddSeconds(validatePeriod);
                if (UseSavedPolicy)
                {
                    BlobContainerPermissions bp = container.GetPermissions();
                    bp.SharedAccessPolicies.Clear();
                    bp.SharedAccessPolicies.Add(PolicySignedIdentifier, sap);
                    container.SetPermissions(bp);
                    SAS = container.GetSharedAccessSignature(new SharedAccessBlobPolicy(), PolicySignedIdentifier);

                    DMLibTestHelper.WaitForACLTakeEffect();
                }
                else
                {
                    SAS = container.GetSharedAccessSignature(sap);
                }
                Test.Info("The SAS is {0}", SAS);
                return SAS;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Clear the SAS policy set to a container, used to revoke the SAS
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <returns>True for success</returns>
        public bool ClearSASPolicyofContainer(string containerName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                BlobContainerPermissions bp = container.GetPermissions();
                bp.SharedAccessPolicies.Clear();
                container.SetPermissions(bp);
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public bool CleanupContainer(string containerName)
        {
            string blobname = string.Empty;
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (!container.Exists())
                    return true;
                IEnumerable<IListBlobItem> blobs = container.ListBlobs(null, true, BlobListingDetails.All, HelperConst.DefaultBlobOptions);
                if (blobs != null)
                {
                    if (blobs.Count() > 500)
                    {
                        return CleanupContainerByRecreateIt(containerName);
                    }
                    foreach (CloudBlob blob in blobs)
                    {
                        blobname = blob.Name;
                        if (blob == null) continue;
                        if (!blob.Exists())
                        {
                            try
                            {
                                blob.Delete(DeleteSnapshotsOption.IncludeSnapshots);
                                continue;
                            }
                            catch (Exception)
                            {
                                continue;
                            }
                        }
                        try
                        {
                            blob.Delete(DeleteSnapshotsOption.IncludeSnapshots);
                        }
                        catch (Exception)
                        {
                            blob.Delete(DeleteSnapshotsOption.None);
                        }
                    }
                }

                Thread.Sleep(5 * 1000);
                if (container.ListBlobs(null, true, BlobListingDetails.All, HelperConst.DefaultBlobOptions).Any())
                {
                    Test.Warn("The container hasn't been cleaned actually.");
                    Test.Info("Trying to cleanup the container by recreating it...");
                    return CleanupContainerByRecreateIt(containerName);
                }
                else
                {
                    return true;
                }
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e) || Helper.IsConflictStorageException(e) || e is OperationCanceledException)
            {
                return CleanupContainerByRecreateIt(containerName);
            }
        }

        public bool CleanupContainerByRecreateIt(string containerName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (container == null || !container.Exists()) return false;

                BlobRequestOptions bro = new BlobRequestOptions();
                bro.RetryPolicy = new LinearRetry(new TimeSpan(0, 1, 0), 3);

                try
                {
                    container.Delete(null, bro);
                }
                catch (Exception e) when (Helper.IsNotFoundStorageException(e))
                { }

                Test.Info("container deleted.");
                bro.RetryPolicy = new LinearRetry(new TimeSpan(0, 3, 0), 3);

                bool createSuccess = false;
                int retry = 0;
                while (!createSuccess && retry++ < 100) //wait up to 5 minutes
                {
                    try
                    {
                        container.Create(bro);
                        createSuccess = true;
                        Test.Info("container recreated.");
                    }
                    catch (Exception e) when (Helper.IsConflictStorageException(e))
                    {
                        Thread.Sleep(3000);
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        // upload all files & dirs(including empty dir) in a local directory to a blob directory
        public void UploadDirectory(string localDirName, string containerName, string blobDirName, bool recursive, string blobType = BlobType.Block)
        {
            DirectoryInfo srcDir = new DirectoryInfo(localDirName);
            CloudBlobDirectory destDir = QueryBlobDirectory(containerName, blobDirName);
            Test.Assert(null != destDir, "dest blob directory exists");

            UploadDirectory(srcDir, destDir, recursive, blobType);
        }

        public void UploadDirectory(DirectoryInfo sourceDir, CloudBlobDirectory destDir, bool recursive, string blobType = BlobType.Block)
        {
            Parallel.ForEach(
                sourceDir.EnumerateFiles(),
                fi =>
                {
                    string fileName = Path.GetFileName(fi.Name);
                    CloudBlob blob = GetCloudBlobReference(destDir, fileName, blobType);
                    bool uploaded = UploadFileToBlob(destDir.Container.Name, blob.Name, blobType, fi.FullName);
                    if (!uploaded)
                    {
                        Test.Assert(false, "failed to upload file:{0}", fi.FullName);
                    }
                });

            if (recursive)
            {
                foreach (DirectoryInfo di in sourceDir.EnumerateDirectories())
                {
                    string subDirName = Path.GetFileName(di.Name);
                    CloudBlobDirectory subDir = destDir.GetDirectoryReference(subDirName);
                    UploadDirectory(di, subDir, true);
                }
            }
        }

        // upload all files & dirs(including empty dir) in a local directory to a blob directory
        public void UploadDirectoryIfNotExist(string localDirName, string containerName, string blobDirName, bool recursive, string blobType = BlobType.Block)
        {
            DirectoryInfo srcDir = new DirectoryInfo(localDirName);
            CloudBlobDirectory destDir = QueryBlobDirectory(containerName, blobDirName);

            UploadDirectoryIfNotExist(srcDir, destDir, recursive, blobType);
        }

        public void UploadDirectoryIfNotExist(DirectoryInfo sourceDir, CloudBlobDirectory destDir, bool recursive, string blobType = BlobType.Block)
        {
            Dictionary<string, CloudBlob> blobs = new Dictionary<string, CloudBlob>();

            foreach (IListBlobItem blobItem in destDir.ListBlobs(true))
            {
                CloudBlob blob = blobItem as CloudBlob;

                if (null != blob)
                {
                    if (MapStorageBlobTypeToBlobType(blob.BlobType) == blobType)
                    {
                        blob.Delete();
                    }
                    else
                    {
                        blobs.Add(blob.Name.Substring(destDir.Prefix.Length), blob);
                    }
                }
            }

            foreach (FileInfo fi in sourceDir.EnumerateFiles())
            {
                string fileName = Path.GetFileName(fi.Name);
                CloudBlob blob;

                if (blobs.TryGetValue(fileName, out blob)
                    && (Helper.GetFileContentMD5(fi.Name) == blob.Properties.ContentMD5))
                {
                    continue;
                }

                blob = GetCloudBlobReference(destDir, fileName, blobType);
                bool uploaded = UploadFileToBlob(destDir.Container.Name, blob.Name, blobType, fi.FullName);
                if (!uploaded)
                {
                    Test.Assert(false, "failed to upload file:{0}", fi.FullName);
                }
            }

            if (recursive)
            {
                foreach (DirectoryInfo di in sourceDir.EnumerateDirectories())
                {
                    string subDirName = Path.GetFileName(di.Name);
                    CloudBlobDirectory subDir = destDir.GetDirectoryReference(subDirName);
                    UploadDirectory(di, subDir, true);
                }
            }
        }

        // compare blob directory with a local LongPathDirectoryExtention. return true only if
        // 1. all files under both dir are the same, and
        // 2. all sub directories under both dir are the same
        public bool CompareCloudBlobDirAndLocalDir(string containerName, string blobDirName, string localDirName)
        {
            try
            {
                CloudBlobDirectory dir = QueryBlobDirectory(containerName, blobDirName);
                if (null == dir)
                {
                    return false;
                }

                return CompareCloudBlobDirAndLocalDir(dir, localDirName);
            }
            catch
            {
                return false;
            }
        }

        public static bool CompareCloudBlobDirAndLocalDir(CloudBlobDirectory dir, string localDirName)
        {
            if (!LongPathDirectoryExtension.Exists(localDirName))
            {
                // return false if local dir not exist.
                Test.Info("dir not exist. local dir={0}", localDirName);
                return false;
            }

            HashSet<string> localSubFiles = new HashSet<string>();
            foreach (string localSubFile in Directory.EnumerateFiles(localDirName))
            {
                localSubFiles.Add(Path.GetFileName(localSubFile));
            }

            HashSet<string> localSubDirs = new HashSet<string>();
            foreach (string localSubDir in Directory.EnumerateDirectories(localDirName))
            {
                localSubDirs.Add(Path.GetFileName(localSubDir));
            }

            foreach (IListBlobItem item in dir.ListBlobs())
            {
                if (item is CloudBlob)
                {
                    CloudBlob tmpBlob = item as CloudBlob;

                    string tmpFileName = Path.GetFileName(tmpBlob.Name);
                    if (!localSubFiles.Remove(tmpFileName))
                    {
                        Test.Info("file not found at local: {0}", tmpBlob.Name);
                        return false;
                    }

                    if (!CompareCloudBlobAndLocalFile(tmpBlob, Path.Combine(localDirName, tmpFileName)))
                    {
                        Test.Info("file content not consistent: {0}", tmpBlob.Name);
                        return false;
                    }
                }
                else if (item is CloudBlobDirectory)
                {
                    CloudBlobDirectory tmpDir = item as CloudBlobDirectory;
                    string tmpDirName = tmpDir.Prefix.TrimEnd(new char[] { '/' });
                    tmpDirName = Path.GetFileName(tmpDirName);

                    if (!localSubDirs.Remove(tmpDirName))
                    {
                        Test.Info("dir not found at local: {0}", tmpDirName);
                        return false;
                    }

                    if (!CompareCloudBlobDirAndLocalDir(tmpDir, Path.Combine(localDirName, tmpDirName)))
                    {
                        return false;
                    }
                }
            }

            return (localSubFiles.Count == 0 && localSubDirs.Count == 0);
        }

        public bool CompareCloudBlobAndLocalFile(string containerName, string blobName, string localFileName)
        {
            CloudBlob blob = QueryBlob(containerName, blobName);
            if (null == blob)
            {
                return false;
            }

            return CompareCloudBlobAndLocalFile(blob, localFileName);
        }

        public static bool CompareCloudBlobAndLocalFile(CloudBlob blob, string localFileName)
        {
            if (!blob.Exists() || !File.Exists(localFileName))
            {
                return false;
            }

            blob.FetchAttributes();
            return blob.Properties.ContentMD5 == Helper.GetFileContentMD5(localFileName);
        }

        public static bool CompareCloudBlobAndCloudBlob(CloudBlob blobA, CloudBlob blobB)
        {
            if (blobA == null || blobB == null || !blobA.Exists() || !blobB.Exists())
            {
                return false;
            }

            blobA.FetchAttributes();
            blobB.FetchAttributes();
            return blobA.Properties.ContentMD5 == blobB.Properties.ContentMD5;
        }

        public static string CalculateMD5ByDownloading(CloudBlob blob, bool disableMD5Check = false)
        {
#if DNXCORE50
            const int bufferSize = 4 * 1024 * 1024;
            blob.FetchAttributes();
            long blobSize = blob.Properties.Length;
            byte[] buffer = new byte[bufferSize];


            long index = 0;
            using (IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.MD5))
            {
                do
                {
                    long sizeToRead = blobSize - index < bufferSize ? blobSize - index : bufferSize;

                    if (sizeToRead <= 0)
                    {
                        break;
                    }

                    blob.DownloadRangeToByteArrayAsync(buffer, 0, index, sizeToRead).Wait();
                    index += sizeToRead;
                    hash.AppendData(buffer, 0, (int)sizeToRead);
                }
                while (true);

                return Convert.ToBase64String(hash.GetHashAndReset());
            }
#else
            using (TemporaryTestFolder tempFolder = new TemporaryTestFolder(Guid.NewGuid().ToString()))
            {
                const string tempFileName = "tempFile";
                string tempFilePath = Path.Combine(tempFolder.Path, tempFileName);
                var blobOptions = new BlobRequestOptions();
                blobOptions.DisableContentMD5Validation = disableMD5Check;
                blobOptions.RetryPolicy = HelperConst.DefaultBlobOptions.RetryPolicy.CreateInstance();
                blob.DownloadToFile(tempFilePath, FileMode.OpenOrCreate, options: blobOptions);
                return Helper.GetFileContentMD5(tempFilePath);
            }
#endif
        }

        /// <summary>
        /// Query the blob
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public CloudBlob QueryBlob(string containerName, string blobName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = GetCloudBlobReference(container, blobName);
                //since GetBlobReference method return no null value even if blob is not exist.
                //use FetchAttributes method to confirm the existence of the blob
                blob.FetchAttributes();

                return blob;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }


        public BlobProperties QueryBlobProperties(string containerName, string blobName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = container.GetBlobReference(blobName);
                if (blob == null)
                {
                    return null;
                }
                blob.FetchAttributes();
                return blob.Properties;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        /// <summary>
        /// Query the blob virtual directory
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public CloudBlobDirectory QueryBlobDirectory(string containerName, string blobDirectoryName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (container == null || !container.Exists()) return null;
                CloudBlobDirectory blobDirectory = container.GetDirectoryReference(blobDirectoryName);
                return blobDirectory;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        public static void GeneratePageBlobWithRangedData(CloudPageBlob pageBlob, List<int> ranges, List<int> gaps)
        {
            Helper.GenerateSparseCloudObject(
                ranges,
                gaps,
                createObject: (totalSize) =>
                {
                    pageBlob.Create(totalSize);
                },
                writeUnit: (unitOffset, randomData) =>
                {
                    pageBlob.WritePages(randomData, unitOffset, options: HelperConst.DefaultBlobOptions);
                });

            Helper.PrintPageBlobRanges(pageBlob);

            // Set correct MD5 to page blob
            string md5 = CalculateMD5ByDownloading(pageBlob);
            pageBlob.Properties.ContentMD5 = md5;
            pageBlob.SetProperties(options: HelperConst.DefaultBlobOptions);
        }

        public static void GenerateBlockBlob(CloudBlockBlob blockBlob, List<int> blockSizes)
        {
            int blockIndex = 0;
            List<string> blocksToCommit = new List<string>();
            foreach (int blockSize in blockSizes)
            {
                byte[] blockIdInBytes = System.Text.Encoding.UTF8.GetBytes(blockIndex.ToString("D4"));
                string blockId = Convert.ToBase64String(blockIdInBytes);
                blocksToCommit.Add(blockId);

                using (MemoryStream randomData = Helper.GetRandomData(blockSize))
                {
                    blockBlob.PutBlock(blockId, randomData, null, options: HelperConst.DefaultBlobOptions);
                }

                ++blockIndex;
            }

            // Commit
            blockBlob.PutBlockList(blocksToCommit, options: HelperConst.DefaultBlobOptions);

            Helper.PrintBlockBlobBlocks(blockBlob);

            // Set correct MD5 to block blob
            string md5 = CloudBlobHelper.CalculateMD5ByDownloading(blockBlob);
            blockBlob.Properties.ContentMD5 = md5;
            blockBlob.SetProperties(options: HelperConst.DefaultBlobOptions);
        }

        /// <summary>
        /// Create or update a blob by its name
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName">the name of the blob</param>
        /// <param name="content">the content to the blob</param>
        /// <returns>Return true on success, false if unable to create, throw exception on error</returns>
        public bool PutBlob(string containerName, string blobName, string content)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (container == null || !container.Exists()) return false;
                CloudBlob blob = GetCloudBlobReference(container, blobName);
#if DNXCORE50
                using (MemoryStream MStream = new MemoryStream(Encoding.ASCII.GetBytes(content)))
#else
                using (MemoryStream MStream = new MemoryStream(ASCIIEncoding.Default.GetBytes(content)))
#endif
                {
                    blob.UploadFromStream(MStream);
                }

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        /// <summary>
        /// change an exist Blob MD5 hash
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName">the name of the blob</param>
        /// <param name="MD5Hash">the MD5 hash to set, must be a base 64 string</param>
        /// <returns>Return true on success, false if unable to set</returns>
        public bool SetMD5Hash(string containerName, string blobName, string MD5Hash)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = container.GetBlobReference(blobName);
                blob.FetchAttributes();
                blob.Properties.ContentMD5 = MD5Hash;
                blob.SetProperties();
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        /// <summary>
        /// put block list. TODO: implement this for large files
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="blockIds"></param>
        /// <returns></returns>
        public bool PutBlockList(string containerName, string blobName, string[] blockIds)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (container == null || !container.Exists()) return false;
                CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

                blob.PutBlockList(blockIds);

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public static string MapStorageBlobTypeToBlobType(StorageBlobType storageBlobType)
        {
            switch (storageBlobType)
            {
                case StorageBlobType.BlockBlob:
                    return BlobType.Block;
                case StorageBlobType.PageBlob:
                    return BlobType.Page;
                case StorageBlobType.AppendBlob:
                    return BlobType.Append;
                default:
                    throw new ArgumentException("storageBlobType");
            }
        }

        public static StorageBlobType MapBlobTypeToStorageBlobType(string blobType)
        {
            switch (blobType)
            {
                case BlobType.Block:
                    return StorageBlobType.BlockBlob;
                case BlobType.Page:
                    return StorageBlobType.PageBlob;
                case BlobType.Append:
                    return StorageBlobType.AppendBlob;
                default:
                    throw new ArgumentException("blobType");
            }
        }

        public static CloudBlob GetCloudBlobReference(CloudBlobContainer container, string blobName, string blobType)
        {
            switch (blobType)
            {
                case BlobType.Block:
                    return container.GetBlockBlobReference(blobName);

                case BlobType.Page:
                    return container.GetPageBlobReference(blobName);

                case BlobType.Append:
                    return container.GetAppendBlobReference(blobName);

                default:
                    throw new ArgumentException("blobType");
            }
        }

        public static CloudBlob GetCloudBlobReference(CloudBlobContainer container, string blobName)
        {
            CloudBlob cloudBlob = container.GetBlobReference(blobName);
            cloudBlob.FetchAttributes();

            return GetCloudBlobReference(container, blobName, MapStorageBlobTypeToBlobType(cloudBlob.Properties.BlobType));
        }

        public static CloudBlob GetCloudBlobReference(CloudBlobDirectory directory, string blobName, string blobType)
        {
            switch (blobType)
            {
                case BlobType.Block:
                    return directory.GetBlockBlobReference(blobName);

                case BlobType.Page:
                    return directory.GetPageBlobReference(blobName);

                case BlobType.Append:
                    return directory.GetAppendBlobReference(blobName);

                default:
                    throw new ArgumentException("blobType");
            }
        }

        public CloudBlob GetBlobReference(string containerName, string blobName, string blobType = BlobType.Block)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            return GetCloudBlobReference(container, blobName, blobType);
        }

        public CloudBlobDirectory GetDirReference(string containerName, string dirName)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            return container.GetDirectoryReference(dirName);
        }

        /// <summary>
        /// Download Blob text by the blob name
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName"></param>
        /// <param name="content"></param>
        /// <returns></returns>
        public bool GetBlob(string containerName, string blobName, out string content)
        {
            content = null;

            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = container.GetBlobReference(blobName);
                //content = blob.DownloadText();
                string tempfile = "temp.txt";
                using (FileStream fs = LongPathFileExtension.Open(tempfile, FileMode.Create))
                {
                    blob.DownloadToStream(fs);
#if DNXCORE50
                    fs.Dispose();
#else
                    fs.Close();
#endif
                }
                content = File.ReadAllText(tempfile);
                File.Delete(tempfile);

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        /// <summary>
        /// Delete a blob by its name
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName">the name of the blob</param>
        /// <returns>Return true on success, false if blob not found, throw exception on error</returns>
        public bool DeleteBlob(string containerName, string blobName)
        {
            blobName = blobName.Replace("\\", "/");
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                if (container.Exists())
                {
                    IEnumerable<IListBlobItem> blobs = container.ListBlobs(blobName, true, BlobListingDetails.All, HelperConst.DefaultBlobOptions);
                    foreach (CloudBlob blob in blobs)
                    {
                        if (blob.Name == blobName)
                        {
                            return blob.DeleteIfExists();
                        }
                    }
                }
                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public bool DeleteBlobDirectory(string containerName, string blobDirectoryName, bool recursive)
        {
            try
            {
                if (blobDirectoryName == string.Empty)
                    return true;
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlobDirectory blobDirectory = container.GetDirectoryReference(blobDirectoryName);

                const int MaxRetryCount = 10;
                int retryCount = 0;
                while (true)
                {
                    bool hasBlobDeleted = false;
                    if (recursive)
                    {
                        foreach (CloudBlob blob in blobDirectory.ListBlobs(recursive, BlobListingDetails.All))
                        {
                            blob.Delete();
                            hasBlobDeleted = true;
                        }
                    }
                    else
                    {
                        foreach (CloudBlob blob in blobDirectory.ListBlobs(recursive))
                        {
                            blob.Delete();
                            hasBlobDeleted = true;
                        }
                    }

                    retryCount++;

                    if (!hasBlobDeleted)
                    {
                        // Return from the method until no blob is listed.
                        break;
                    }
                    else
                    {
                        if (retryCount > MaxRetryCount)
                        {
                            Test.Error("Cannot delete the blob directory within max retry count");
                            return false;
                        }

                        // Wait for some time, and then attempt to delete all listed blobs again.
                        Thread.Sleep(5 * 1000);
                    }
                }

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }

        public bool UploadFileToBlockBlob(string containerName, string blobName, string filePath)
        {
            return UploadFileToBlob(containerName, blobName, BlobType.Block, filePath);
        }

        public bool UploadFileToPageBlob(string containerName, string blobName, string filePath)
        {
            return UploadFileToBlob(containerName, blobName, BlobType.Page, filePath);
        }

        public bool UploadFileToAppendBlob(string containerName, string blobName, string filePath)
        {
            return UploadFileToBlob(containerName, blobName, BlobType.Append, filePath);
        }

        public bool UploadFileToBlob(string containerName, string blobName, string blobType, string filePath)
        {
            CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
            BlobRequestOptions options = new BlobRequestOptions
            {
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(90), 3),
                StoreBlobContentMD5 = true,
            };

            container.CreateIfNotExists(options);

            CloudBlob blob = GetCloudBlobReference(container, blobName, blobType);
            blob.UploadFromFile(filePath, null, options, null);
            Test.Info("block blob {0} has been uploaded successfully", blob.Name);

            return true;
        }

        public bool DownloadFile(string containerName, string blobName, string filePath)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                BlobRequestOptions bro = new BlobRequestOptions();
                bro.RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 30), 3);
                bro.ServerTimeout = new TimeSpan(1, 30, 0);
                bro.MaximumExecutionTime = new TimeSpan(1, 30, 0);
                CloudBlob blob = container.GetBlobReference(blobName);

                using (FileStream fs = LongPathFileExtension.Open(filePath, FileMode.Create))
                {
                    blob.DownloadToStream(fs, null, bro);
#if DNXCORE50
                    fs.Dispose();
#else
                    fs.Close();
#endif
                }

                return true;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return false;
            }
        }
        /// <summary>
        /// Creates a snapshot of the blob
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName">the name of blob</param>
        /// <returns>blob snapshot</returns>
        public CloudBlob CreateSnapshot(string containerName, string blobName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = GetCloudBlobReference(container, blobName);
                if (blob.Properties.BlobType == Microsoft.Azure.Storage.Blob.BlobType.BlockBlob)
                {
                    CloudBlockBlob BBlock = blob as CloudBlockBlob;
                    return BBlock.Snapshot();
                }
                else
                {
                    CloudPageBlob BBlock = blob as CloudPageBlob;
                    return BBlock.Snapshot();
                }

            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return null;
            }
        }

        /// <summary>
        /// delete snapshot of the blob (DO NOT delete blob)
        /// </summary>
        /// <param name="containerName">the name of the container</param>
        /// <param name="blobName">the name of blob</param>
        /// <returns></returns>
        public void DeleteSnapshotOnly(string containerName, string blobName)
        {
            try
            {
                CloudBlobContainer container = BlobClient.GetContainerReference(containerName);
                CloudBlob blob = container.GetBlobReference(blobName);

                //Indicate that any snapshots should be deleted.
                blob.Delete(DeleteSnapshotsOption.DeleteSnapshotsOnly);
                return;
            }
            catch (Exception e) when (Helper.IsNotFoundStorageException(e))
            {
                return;
            }
        }
        /// <summary>
        /// return name of snapshot
        /// </summary>
        /// <param name="fileName">the name of blob</param>
        /// <param name="snapShot">A blob snapshot</param>
        /// <returns>name of snapshot</returns>
        public string GetNameOfSnapshot(string fileName, CloudBlob snapshot)
        {
            string fileNameNoExt = Path.GetFileNameWithoutExtension(fileName);
            string extension = Path.GetExtension(fileName);
            string timeStamp = string.Format("{0:yyyy-MM-dd HHmmss fff}", snapshot.SnapshotTime.Value);
            return string.Format("{0} ({1}){2}", fileNameNoExt, timeStamp, extension);
        }
    }
    internal class TemporaryTestFile : IDisposable
    {
        private const int DefaultSizeInKB = 1;
        private bool disposed = false;

        public string Path
        {
            get;
            private set;
        }

        public int Size
        {
            get;
            private set;
        }

        public TemporaryTestFile(string path)
            : this(path, DefaultSizeInKB)
        {
        }

        public TemporaryTestFile(string path, int sizeInKB)
        {
            Path = path;
            Size = sizeInKB;

            if (File.Exists(path))
            {
                Test.Assert(false, "file {0} already exist", path);
            }

            Helper.GenerateRandomTestFile(Path, Size);
        }

        ~TemporaryTestFile()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                try
                {
                    Helper.DeleteFile(Path);
                }
                catch
                {
                }

                disposed = true;
            }
        }
    }

    internal class TemporaryTestFolder : IDisposable
    {
        private bool disposed = false;

        public string Path
        {
            get;
            private set;
        }

        public TemporaryTestFolder(string path)
        {
            Path = path;

            if (LongPathDirectoryExtension.Exists(path))
            {
                Test.Assert(false, "folder {0} already exist", path);
            }

            Helper.CreateNewFolder(path);
        }

        ~TemporaryTestFolder()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                try
                {
                    Helper.DeleteFolder(Path);
                }
                catch
                {
                }

                disposed = true;
            }
        }
    }

    public static class CrossPlatformHelpers
    {
        public static bool IsWindows
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#else
                return true;
#endif // RUNTIME_INFORMATION
            }
        }

        public static bool IsOSX
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
#else
                return false;
#endif // RUNTIME_INFORMATION
            }
        }

        public static bool IsLinux
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
#else
                return false;
#endif // RUNTIME_INFORMATION
            }
        }
    }
}
