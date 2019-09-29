//------------------------------------------------------------------------------
// <copyright file="DMLibTestBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    using XSCLBlobType = Microsoft.Azure.Storage.Blob.BlobType;

    public class DMLibTestBase : MultiDirectionTestBase<DMLibDataInfo, DMLibDataType>
    {
        private static Dictionary<string, string> sourceConnectionStrings = new Dictionary<string, string>();
        private static Dictionary<string, string> destConnectionStrings = new Dictionary<string, string>();

        public const string FolderName = "folder";
        public const string FileName = "testfile";
        public const string DirName = "testdir";

        public static int FileSizeInKB
        {
            get;
            set;
        }

        public static void SetSourceConnectionString(string value, DMLibDataType dataType)
        {
            string key = DMLibTestBase.GetLocationKey(dataType);
            sourceConnectionStrings[key] = value;
        }

        public static void SetDestConnectionString(string value, DMLibDataType dataType)
        {
            string key = DMLibTestBase.GetLocationKey(dataType);
            destConnectionStrings[key] = value;
        }

        public static string GetSourceConnectionString(DMLibDataType dataType)
        {
            return GetConnectionString(SourceOrDest.Source, dataType);
        }

        public static string GetDestConnectionString(DMLibDataType dataType)
        {
            return GetConnectionString(SourceOrDest.Dest, dataType);
        }

        private static string GetConnectionString(SourceOrDest sourceOrDest, DMLibDataType dataType)
        {
            IDictionary<string, string> connectionStrings = SourceOrDest.Source == sourceOrDest ? sourceConnectionStrings : destConnectionStrings;
            string key = DMLibTestBase.GetLocationKey(dataType);

            string connectionString;
            if (connectionStrings.TryGetValue(key, out connectionString))
            {
                return connectionString;
            }

            if (SourceOrDest.Dest == sourceOrDest)
            {
                return TestAccounts.Secondary.ConnectionString;
            }
            else
            {
                return TestAccounts.Primary.ConnectionString;
            }
        }

        public static new void BaseClassInitialize(TestContext testContext)
        {
            MultiDirectionTestBase<DMLibDataInfo, DMLibDataType>.BaseClassInitialize(testContext);
            FileSizeInKB = int.Parse(Test.Data.Get("FileSize"));
            DMLibTestBase.InitializeDataAdaptor();
        }

        private static void InitializeDataAdaptor()
        {
            var srcBlobTestAccount = new TestAccount(GetSourceConnectionString(DMLibDataType.CloudBlob));
            var destBlobTestAccount = new TestAccount(GetDestConnectionString(DMLibDataType.CloudBlob));

            var srcFileTestAccount = new TestAccount(GetSourceConnectionString(DMLibDataType.CloudFile));
            var destFileTestAccount = new TestAccount(GetDestConnectionString(DMLibDataType.CloudFile));

            // Initialize data adaptor for normal location
            SetSourceAdaptor(DMLibDataType.Local, new LocalDataAdaptor(DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Source));
            SetSourceAdaptor(DMLibDataType.Stream, new LocalDataAdaptor(DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Source, useStream: true));
            SetSourceAdaptor(DMLibDataType.URI, new URIBlobDataAdaptor(srcBlobTestAccount, DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix()));
            SetSourceAdaptor(DMLibDataType.BlockBlob, new CloudBlobDataAdaptor(srcBlobTestAccount, DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Block, SourceOrDest.Source));
            SetSourceAdaptor(DMLibDataType.PageBlob, new CloudBlobDataAdaptor(srcBlobTestAccount, DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Page, SourceOrDest.Source));
            SetSourceAdaptor(DMLibDataType.AppendBlob, new CloudBlobDataAdaptor(srcBlobTestAccount, DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Append, SourceOrDest.Source));
            SetSourceAdaptor(DMLibDataType.CloudFile, new CloudFileDataAdaptor(srcFileTestAccount, DMLibTestBase.SourceRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Source));

            SetDestAdaptor(DMLibDataType.Local, new LocalDataAdaptor(DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Dest));
            SetDestAdaptor(DMLibDataType.Stream, new LocalDataAdaptor(DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Dest, useStream: true));
            SetDestAdaptor(DMLibDataType.BlockBlob, new CloudBlobDataAdaptor(destBlobTestAccount, DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Block, SourceOrDest.Dest));
            SetDestAdaptor(DMLibDataType.PageBlob, new CloudBlobDataAdaptor(destBlobTestAccount, DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Page, SourceOrDest.Dest));
            SetDestAdaptor(DMLibDataType.AppendBlob, new CloudBlobDataAdaptor(destBlobTestAccount, DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), BlobType.Append, SourceOrDest.Dest));
            SetDestAdaptor(DMLibDataType.CloudFile, new CloudFileDataAdaptor(destFileTestAccount, DMLibTestBase.DestRoot + DMLibTestHelper.RandomNameSuffix(), SourceOrDest.Dest));
        }

        public TestResult<DMLibDataInfo> ExecuteTestCase(DMLibDataInfo sourceDataInfo, TestExecutionOptions<DMLibDataInfo> options)
        {
            if (options.DisableSourceCleaner)
                this.CleanupData(false, true);
            else
                this.CleanupData();
            SourceAdaptor.CreateIfNotExists();
            DestAdaptor.CreateIfNotExists();

            string sourceRootPath = string.Empty;
            DirNode sourceRootNode = new DirNode(string.Empty);
            if (sourceDataInfo != null)
            {
                sourceRootPath = sourceDataInfo.RootPath;
                sourceRootNode = sourceDataInfo.RootNode;
                if (!options.DisableSourceGenerator)
                    SourceAdaptor.GenerateData(sourceDataInfo);
            }

            string destRootPath = string.Empty;
            if (options.DestTransferDataInfo != null)
            {
                destRootPath = options.DestTransferDataInfo.RootPath;
                DestAdaptor.GenerateData(options.DestTransferDataInfo);
            }

            if (options.AfterDataPrepared != null)
            {
                options.AfterDataPrepared();
            }

            List<TransferItem> allItems = new List<TransferItem>();

            if (options.IsDirectoryTransfer)
            {
                TransferItem item = new TransferItem()
                {
                    SourceObject = SourceAdaptor.GetTransferObject(sourceRootPath, sourceRootNode, options.SourceCredentials),
                    DestObject = DestAdaptor.GetTransferObject(destRootPath, sourceRootNode, options.DestCredentials),
                    SourceType = DMLibTestContext.SourceType,
                    DestType = DMLibTestContext.DestType,
                    CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                    IsDirectoryTransfer = true,
                };

                if (options.TransferItemModifier != null)
                {
                    options.TransferItemModifier(null, item);
                }

                allItems.Add(item);
            }
            else
            {
                foreach (var fileNode in sourceDataInfo.EnumerateFileNodes())
                {
                    TransferItem item = new TransferItem()
                    {
                        SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, fileNode, options.SourceCredentials),
                        DestObject = DestAdaptor.GetTransferObject(destRootPath, fileNode, options.DestCredentials),
                        SourceType = DMLibTestContext.SourceType,
                        DestType = DMLibTestContext.DestType,
                        CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                    };

                    if (options.TransferItemModifier != null)
                    {
                        options.TransferItemModifier(fileNode, item);
                    }

                    allItems.Add(item);
                }
            }

            return this.RunTransferItems(allItems, options);
        }

        public TestResult<DMLibDataInfo> RunTransferItems(List<TransferItem> items, TestExecutionOptions<DMLibDataInfo> options)
        {
            Dictionary<TransferItem, Task<TransferStatus>> allTasks = new Dictionary<TransferItem, Task<TransferStatus>>();
            var testResult = new TestResult<DMLibDataInfo>();
            testResult.TransferItems = items;
            
            try
            {
                foreach (TransferItem item in items)
                {
                    DMLibWrapper wrapper = GetDMLibWrapper(item.SourceType, item.DestType);

                    if (item.BeforeStarted != null)
                    {
                        item.BeforeStarted();
                    }

                    try
                    {
                        if (options.LimitSpeed)
                        {
                            OperationContext.GlobalSendingRequest += this.LimitSpeed;
                            TransferManager.Configurations.ParallelOperations = DMLibTestConstants.LimitedSpeedNC;
                        }

                        if (options.BlockSize.HasValue)
                        {
                            TransferManager.Configurations.BlockSize = options.BlockSize.Value;
                        }

                        allTasks.Add(item, wrapper.DoTransfer(item));
                    }
                    catch (Exception e)
                    {
                        testResult.AddException(e);
                    }

                    if (item.AfterStarted != null)
                    {
                        item.AfterStarted();
                    }
                }

                if (options.AfterAllItemAdded != null)
                {
                    options.AfterAllItemAdded();
                }

                try
                {
                    if (!Task.WaitAll(allTasks.Values.ToArray(), options.TimeoutInMs))
                    {
                        Test.Error("Running transfer items timed out");
                    }
                }
                catch (Exception e)
                {
                    AggregateException ae = e as AggregateException;
                    if (ae != null)
                    {
                        ae = ae.Flatten();
                        foreach (var innerE in ae.InnerExceptions)
                        {
                            testResult.AddException(innerE);
                        }
                    }
                    else
                    {
                        testResult.AddException(e);
                    }
                }
            }
            finally
            {
                if (options.LimitSpeed)
                {
                    OperationContext.GlobalSendingRequest -= this.LimitSpeed;
                    TransferManager.Configurations.ParallelOperations = DMLibTestConstants.DefaultNC;
                }
                TransferManager.Configurations.BlockSize = DMLibTestConstants.DefaultBlockSize;
            }

            Parallel.ForEach(allTasks, pair =>
            {
                TransferItem transferItem = pair.Key;
                Task<TransferStatus> task = pair.Value;

                transferItem.CloseStreamIfNecessary();

                try
                {
                    transferItem.FinalStatus = task.Result;
                }
                catch (Exception e)
                {
                    transferItem.Exception = e;
                }
            });

            if (!options.DisableDestinationFetch)
            {
                testResult.DataInfo = DestAdaptor.GetTransferDataInfo(string.Empty);
            }

            foreach (var exception in testResult.Exceptions)
            {
                Test.Info("Exception from DMLib: {0}", exception.ToString());
            }

            return testResult;
        }


        public void ValidateDestinationMD5ByDownloading(DMLibDataInfo destDataInfo, TestExecutionOptions<DMLibDataInfo> options)
        {
            foreach (var fileNode in destDataInfo.EnumerateFileNodes())
            {
                var dest = DestAdaptor.GetTransferObject(destDataInfo.RootPath, fileNode, options.DestCredentials);
                DestAdaptor.ValidateMD5ByDownloading(dest);
            }
        }

        public DMLibWrapper GetDMLibWrapper(DMLibDataType sourceType, DMLibDataType destType)
        {
            if (DMLibTestBase.IsLocal(sourceType))
            {
                return new UploadWrapper();
            }
            else if (DMLibTestBase.IsLocal(destType))
            {
                return new DownloadWrapper();
            }
            else
            {
                return new CopyWrapper();
            }
        }

        public static object DefaultTransferOptions
        {
            get
            {
                return DMLibTestBase.GetDefaultTransferOptions(DMLibTestContext.SourceType, DMLibTestContext.DestType);
            }
        }

        public static object DefaultTransferDirectoryOptions
        {
            get
            {
                return DMLibTestBase.GetDefaultTransferDirectoryOptions(DMLibTestContext.SourceType, DMLibTestContext.DestType);
            }
        }

        public static object GetDefaultTransferOptions(DMLibDataType sourceType, DMLibDataType destType)
        {
            if (DMLibTestBase.IsLocal(sourceType))
            {
                return new UploadOptions();
            }
            else if (DMLibTestBase.IsLocal(destType))
            {
                return new DownloadOptions();
            }
            else
            {
                return new CopyOptions();
            }
        }

        public static object GetDefaultTransferDirectoryOptions(DMLibDataType sourceType, DMLibDataType destType)
        {
            if (DMLibTestBase.IsLocal(sourceType))
            {
                var result = new UploadDirectoryOptions();

                if (IsCloudBlob(destType))
                {
                    result.BlobType = MapBlobDataTypeToXSCLBlobType(destType);
                }

                return result;
            }
            else if (DMLibTestBase.IsLocal(destType))
            {
                return new DownloadDirectoryOptions();
            }
            else
            {
                var result = new CopyDirectoryOptions();

                if (IsCloudBlob(destType))
                {
                    result.BlobType = MapBlobDataTypeToXSCLBlobType(destType);
                }

                return result;
            }
        }

        public static string MapBlobDataTypeToBlobType(DMLibDataType blobDataType)
        {
            switch (blobDataType)
            {
                case DMLibDataType.BlockBlob:
                    return BlobType.Block;
                case DMLibDataType.PageBlob:
                    return BlobType.Page;
                case DMLibDataType.AppendBlob:
                    return BlobType.Append;
                default:
                    throw new ArgumentException("blobDataType");
            }
        }

        public static XSCLBlobType MapBlobDataTypeToXSCLBlobType(DMLibDataType blobDataType)
        {
            switch (blobDataType)
            {
                case DMLibDataType.BlockBlob:
                    return XSCLBlobType.BlockBlob;
                case DMLibDataType.PageBlob:
                    return XSCLBlobType.PageBlob;
                case DMLibDataType.AppendBlob:
                    return XSCLBlobType.AppendBlob;
                default:
                    throw new ArgumentException("blobDataType");
            }
        }

        private void LimitSpeed(object sender, RequestEventArgs e)
        {
            Test.Info("LimitSpeeding");
            Thread.Sleep(100);
        }

        public DMLibDataInfo GenerateSourceDataInfo(FileNumOption fileNumOption, string folderName = "")
        {
            return this.GenerateSourceDataInfo(fileNumOption, DMLibTestBase.FileSizeInKB, folderName);
        }

        public DMLibDataInfo GenerateSourceDataInfo(FileNumOption fileNumOption, int totalSizeInKB, string folderName = "")
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(folderName);

            if (fileNumOption == FileNumOption.FileTree)
            {
                DMLibDataHelper.AddTreeTotalSize(
                    sourceDataInfo.RootNode,
                    DMLibTestBase.DirName,
                    DMLibTestBase.FileName,
                    DMLibTestConstants.RecursiveFolderWidth,
                    DMLibTestConstants.RecursiveFolderDepth,
                    totalSizeInKB);
            }
            else if (fileNumOption == FileNumOption.FlatFolder)
            {
                DMLibDataHelper.AddMultipleFilesTotalSize(
                    sourceDataInfo.RootNode,
                    DMLibTestBase.FileName,
                    DMLibTestConstants.FlatFileCount,
                    totalSizeInKB);
            }
            else if (fileNumOption == FileNumOption.OneFile)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, totalSizeInKB);
            }

            return sourceDataInfo;
        }

        public enum FileNumOption
        {
            OneFile,
            FlatFolder,
            FileTree,
        }

        public override bool IsCloudService(DMLibDataType dataType)
        {
            return DMLibDataType.Cloud.HasFlag(dataType);
        }

        public static bool IsLocal(DMLibDataType dataType)
        {
            return dataType == DMLibDataType.Stream || dataType == DMLibDataType.Local;
        }

        public static bool IsCloudBlob(DMLibDataType dataType)
        {
            return DMLibDataType.CloudBlob.HasFlag(dataType);
        }
    }
}
