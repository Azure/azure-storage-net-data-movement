//------------------------------------------------------------------------------
// <copyright file="DMLibTestHelper.cs" company="Microsoft">
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
    using System.Runtime.Serialization;
#if BINARY_SERIALIZATION
    using System.Runtime.Serialization.Formatters.Binary;
#endif
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;
    using MS.Test.Common.MsTestLib;
    using StorageBlob = Microsoft.Azure.Storage.Blob;

    public enum FileSizeUnit
    {
        B,
        KB,
        MB,
        GB,
    }

    public enum TestAgainst
    {
        PublicAzure,
        TestTenant,
        DevFabric
    }

    public class SummaryInterval
    {
        /// <summary>
        /// Min value which is inclusive.
        /// </summary>
        private int minValue;

        /// <summary>
        /// Max value which is inclusive.
        /// </summary>
        private int maxValue;

        public SummaryInterval(int minValue, int maxValue)
        {
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public int MinValue
        {
            get
            {
                return this.minValue;
            }
        }

        public int MaxValue
        {
            get
            {
                return this.maxValue;
            }
        }

        public bool InsideInterval(int value)
        {
            return value >= this.minValue && value <= this.maxValue;
        }
    }

    public static class DMLibTestHelper
    {
        public static CopyMethod ToCopyMethod(this DMLibCopyMethod copyMethod)
        {
            switch (copyMethod)
            {
                case DMLibCopyMethod.ServiceSideAsyncCopy:
                    return CopyMethod.ServiceSideAsyncCopy;
                case DMLibCopyMethod.ServiceSideSyncCopy:
                    return CopyMethod.ServiceSideSyncCopy;
                default:
                    return CopyMethod.SyncCopy;
            }
        }

        private static char[] InvalidFileNameChars = new char[] {
        '"', '<', '>', '|', '\0', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006',
        '\a', '\b', '\t', '\n', '\v', '\f', '\r', '\u000e', '\u000f', '\u0010', '\u0011', '\u0012', '\u0013', '\u0014', '\u0015',
        '\u0016', '\u0017', '\u0018', '\u0019', '\u001a', '\u001b', '\u001c', '\u001d', '\u001e', '\u001f', ':', '*', '?', '\\', '/'};

        private static Random random = new Random();

        private static readonly char[] validSuffixChars = "abcdefghijkjlmnopqrstuvwxyz".ToCharArray();

        public static TransferCheckpoint SaveAndReloadCheckpoint(TransferCheckpoint checkpoint)
        {
            //return checkpoint;
            Test.Info("Save and reload checkpoint");
#if BINARY_SERIALIZATION
            IFormatter formatter = new BinaryFormatter();
#else
            var formatter = new DataContractSerializer(typeof(TransferCheckpoint));
#endif
            TransferCheckpoint reloadedCheckpoint;

            string tempFileName = Guid.NewGuid().ToString();

            using (var stream = new FileStream(tempFileName, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                formatter.Serialize(stream, checkpoint);
            }

            using (var stream = new FileStream(tempFileName, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                reloadedCheckpoint = formatter.Deserialize(stream) as TransferCheckpoint;
            }

            File.Delete(tempFileName);

            return reloadedCheckpoint;
        }

        public static TransferCheckpoint RandomReloadCheckpoint(TransferCheckpoint checkpoint)
        {
            if (Helper.RandomBoolean())
            {
                Test.Info("Save and reload checkpoint");
                return DMLibTestHelper.SaveAndReloadCheckpoint(checkpoint);
            }

            return checkpoint;
        }

        public static void KeepFilesWhenCaseFail(params string[] filesToKeep)
        {
            if (Test.ErrorCount > 0)
            {
                const string debugFilePrefix = "debug_file_";
                string folderName = Guid.NewGuid().ToString();
                Directory.CreateDirectory(folderName);

                Test.Info("Move files to folder {0} for debug.", folderName);

                for (int i = 0; i < filesToKeep.Length; ++i)
                {
                    string debugFileName = debugFilePrefix + i;
                    string debugFilePath = Path.Combine(folderName, debugFileName);
                    File.Move(filesToKeep[i], debugFilePath);

                    Test.Info("{0} ---> {1}", filesToKeep[i], debugFileName);
                }
            }
        }

        public static string RandomContainerName()
        {
            return Test.Data.Get("containerName") + RandomNameSuffix();
        }

        public static string RandomNameSuffix()
        {
            return FileOp.NextString(random, 6, validSuffixChars);
        }

        public static bool WaitForProcessExit(Process p, int timeoutInSecond)
        {
            bool exit = p.WaitForExit(timeoutInSecond * 1000);
            if (!exit)
            {
                Test.Assert(false, "Process {0} should exit in {1} s.", p.ProcessName, timeoutInSecond);
                p.Kill();
                return false;
            }

            return true;
        }

        public static string RandomizeCase(string value)
        {
            return ConvertRandomCharsToUpperCase(value.ToLower());
        }

        public static void UploadFromByteArray(this StorageBlob.CloudBlob cloudBlob, byte[] randomData)
        {
            if (StorageBlob.BlobType.BlockBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudBlockBlob).UploadFromByteArray(randomData, 0, randomData.Length);
            }
            else if (StorageBlob.BlobType.PageBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudPageBlob).UploadFromByteArray(randomData, 0, randomData.Length);
            }
            else if (StorageBlob.BlobType.AppendBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudAppendBlob).UploadFromByteArray(randomData, 0, randomData.Length);
            }
            else
            {
                throw new InvalidOperationException(string.Format("Invalid blob type: {0}", cloudBlob.BlobType));
            }
        }

        public static void UploadFromFile(this StorageBlob.CloudBlob cloudBlob,
            string path,
            AccessCondition accessCondition = null,
            StorageBlob.BlobRequestOptions options = null,
            OperationContext operationContext = null)
        {
            if (StorageBlob.BlobType.BlockBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudBlockBlob).UploadFromFile(path, accessCondition, options, operationContext);
            }
            else if (StorageBlob.BlobType.PageBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudPageBlob).UploadFromFile(path, accessCondition, options, operationContext);
            }
            else if (StorageBlob.BlobType.AppendBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudAppendBlob).UploadFromFile(path, accessCondition, options, operationContext);
            }
            else
            {
                throw new InvalidOperationException(string.Format("Invalid blob type: {0}", cloudBlob.BlobType));
            }
        }

        public static void UploadFromStream(this StorageBlob.CloudBlob cloudBlob, Stream source)
        {
            if (StorageBlob.BlobType.BlockBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudBlockBlob).UploadFromStream(source);
            }
            else if (StorageBlob.BlobType.PageBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudPageBlob).UploadFromStream(source);
            }
            else if (StorageBlob.BlobType.AppendBlob == cloudBlob.BlobType)
            {
                (cloudBlob as StorageBlob.CloudAppendBlob).UploadFromStream(source);
            }
            else
            {
                throw new InvalidOperationException(string.Format("Invalid blob type: {0}", cloudBlob.BlobType));
            }
        }

        public static void WaitForACLTakeEffect()
        {
            if (DMLibTestHelper.GetTestAgainst() != TestAgainst.DevFabric)
            {
                Test.Info("Waiting for 30s to ensure the ACL take effect on server side...");
                Thread.Sleep(30 * 1000);
            }
        }

        public static string RandomProtocol()
        {
            if (0 == new Random().Next(2))
            {
                return Protocol.Http;
            }

            return Protocol.Https;
        }

        public static bool DisableHttps()
        {
            if (DMLibTestHelper.GetTestAgainst() == TestAgainst.TestTenant)
            {
                return true;
            }

            return false;
        }

        public static TestAgainst GetTestAgainst()
        {
            string testAgainst = string.Empty;
            try
            {
                testAgainst = Test.Data.Get("TestAgainst");
            }
            catch
            {
            }

            if (String.Compare(testAgainst, "publicazure", true) == 0)
            {
                return TestAgainst.PublicAzure;
            }
            else if (String.Compare(testAgainst, "testtenant", true) == 0)
            {
                return TestAgainst.TestTenant;
            }
            else if (String.Compare(testAgainst, "devfabric", true) == 0)
            {
                return TestAgainst.DevFabric;
            }

            // Use dev fabric by default
            return TestAgainst.DevFabric;
        }

        public static List<FileAttributes> GetFileAttributesFromParameter(string s)
        {
            List<FileAttributes> Lfa = new List<FileAttributes>();

            if (null == s)
            {
                return Lfa;
            }

            foreach (char c in s)
            {
                switch (c)
                {
                    case 'R':
                        if (!Lfa.Contains(FileAttributes.ReadOnly))
                            Lfa.Add(FileAttributes.ReadOnly);
                        break;
                    case 'A':
                        if (!Lfa.Contains(FileAttributes.Archive))
                            Lfa.Add(FileAttributes.Archive);
                        break;
                    case 'S':
                        if (!Lfa.Contains(FileAttributes.System))
                            Lfa.Add(FileAttributes.System);
                        break;
                    case 'H':
                        if (!Lfa.Contains(FileAttributes.Hidden))
                            Lfa.Add(FileAttributes.Hidden);
                        break;
                    case 'C':
                        if (!Lfa.Contains(FileAttributes.Compressed))
                            Lfa.Add(FileAttributes.Compressed);
                        break;
                    case 'N':
                        if (!Lfa.Contains(FileAttributes.Normal))
                            Lfa.Add(FileAttributes.Normal);
                        break;
                    case 'E':
                        if (!Lfa.Contains(FileAttributes.Encrypted))
                            Lfa.Add(FileAttributes.Encrypted);
                        break;
                    case 'T':
                        if (!Lfa.Contains(FileAttributes.Temporary))
                            Lfa.Add(FileAttributes.Temporary);
                        break;
                    case 'O':
                        if (!Lfa.Contains(FileAttributes.Offline))
                            Lfa.Add(FileAttributes.Offline);
                        break;
                    case 'I':
                        if (!Lfa.Contains(FileAttributes.NotContentIndexed))
                            Lfa.Add(FileAttributes.NotContentIndexed);
                        break;
                    default:
                        break;
                }
            }
            return Lfa;
        }

        public static List<string> GenerateFileWithAttributes(
            string folder,
            string filePrefix,
            int number,
            List<FileAttributes> includeAttributes,
            List<FileAttributes> excludeAttributes,
            int fileSizeInUnit = 1,
            FileSizeUnit unit = FileSizeUnit.KB)
        {
            List<string> fileNames = new List<string>(number);

            for (int i = 0; i < number; i++)
            {
                string fileName = filePrefix + i.ToString();
                string filePath = Path.Combine(folder, fileName);
                fileNames.Add(fileName);

                DMLibTestHelper.PrepareLocalFile(filePath, fileSizeInUnit, unit);

                if (includeAttributes != null)
                    foreach (FileAttributes fa in includeAttributes)
                        FileOp.SetFileAttribute(filePath, fa);
                if (excludeAttributes != null)
                    foreach (FileAttributes fa in excludeAttributes)
                        FileOp.RemoveFileAttribute(filePath, fa);
            }

            return fileNames;
        }

        public static void PrepareLocalFile(string filePath, long fileSizeInUnit, FileSizeUnit fileSizeUnit)
        {
            if (FileSizeUnit.B == fileSizeUnit)
            {
                Helper.GenerateFileInBytes(filePath, fileSizeInUnit);
            }
            else if (FileSizeUnit.KB == fileSizeUnit)
            {
                Helper.GenerateFileInKB(filePath, fileSizeInUnit);
            }
            else if (FileSizeUnit.MB == fileSizeUnit)
            {
                Helper.GenerateFileInMB(filePath, fileSizeInUnit);
            }
            else
            {
                Helper.GenerateFileInGB(filePath, fileSizeInUnit);
            }
        }

        public static bool ContainsIgnoreCase(string baseString, string subString)
        {
            return (baseString.IndexOf(subString, StringComparison.OrdinalIgnoreCase) >= 0);
        }

        private static string ConvertRandomCharsToUpperCase(string input)
        {
            Random rnd = new Random();
            char[] array = input.ToCharArray();

            for (int i = 0; i < array.Length; ++i)
            {
                if (Char.IsLower(array[i]) && rnd.Next() % 2 != 0)
                {
                    array[i] = Char.ToUpper(array[i]);
                }
            }

            return new string(array);
        }

        /// <summary>
        /// Append snapshot time to a file name.
        /// </summary>
        /// <param name="fileName">Original file name.</param>
        /// <param name="snapshotTime">Snapshot time to append.</param>
        /// <returns>A file name with appended snapshot time.</returns>
        public static string AppendSnapShotTimeToFileName(string fileName, DateTimeOffset? snapshotTime)
        {
            string resultName = fileName;

            if (snapshotTime.HasValue)
            {
                string pathAndFileNameNoExt, extension;
                GetBasePathAndExtension(fileName, out pathAndFileNameNoExt, out extension);
                string timeStamp = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0:yyyy-MM-dd HHmmss fff}",
                    snapshotTime.Value);

                resultName = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} ({1}){2}",
                    pathAndFileNameNoExt,
                    timeStamp,
                    extension);
            }

            return resultName;
        }

        private static void GetBasePathAndExtension(string filePath, out string basePath, out string extension)
        {
            int index = filePath.LastIndexOf(".");
            extension = string.Empty;

            if (-1 == index)
            {
                basePath = filePath;
            }
            else
            {
                basePath = filePath.Substring(0, index);

                if (index < filePath.Length - 1)
                {
                    extension = filePath.Substring(index);
                }
            }
        }

        public static string EscapeInvalidCharacters(string fileName)
        {
            // Replace invalid characters with %HH, with HH being the hexadecimal
            // representation of the invalid character.
            foreach (char c in InvalidFileNameChars)
            {
                fileName = fileName.Replace(c.ToString(), string.Format(CultureInfo.InvariantCulture, "%{0:X2}", (int)c));
            }

            return fileName;
        }

        public static string TransferInstanceToString(object transferInstance)
        {
            CloudBlob blob = transferInstance as CloudBlob;

            if (null != blob)
            {
                return blob.SnapshotQualifiedUri.ToString();
            }

            CloudFile file = transferInstance as CloudFile;

            if (null != file)
            {
                return file.Uri.ToString();
            }

            string filePath = transferInstance as string;

            if (null != filePath)
            {
                return filePath;
            }

            Uri uri = transferInstance as Uri;

            if (null != uri)
            {
                return uri.ToString();
            }

            Stream stream = transferInstance as Stream;

            if (null != stream)
            {
                return stream.ToString();
            }

            throw new InvalidOperationException(string.Format("Invalid transfer target: {0}", transferInstance));
        }
    }
}
