using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DMLibTestCodeGen;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.Azure.Storage.File;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MS.Test.Common.MsTestLib;

namespace DMLibTest.Cases
{
    [MultiDirectionTestClass]
    public class SMBPropertiesTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods
#if DNXCORE50
        public SMBPropertiesTest()
        {
            MyTestInitialize();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            MyTestCleanup();
        }
#endif
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: SnapshotTest");
            DMLibTestBase.BaseClassInitialize(testContext);
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            DMLibTestBase.BaseClassCleanup();
        }

        [TestInitialize()]
        public void MyTestInitialize()
        {
            base.BaseTestInitialize();
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            base.BaseTestCleanup();
        }
        #endregion

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestDirectoryPreserveSMBProperties()
        {
            if (!CrossPlatformHelpers.IsWindows) return;
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly,
                CloudFileNtfsAttributes.Hidden,
#if DEBUG
                CloudFileNtfsAttributes.System,
                CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Normal,
                CloudFileNtfsAttributes.Offline,
                CloudFileNtfsAttributes.NotContentIndexed,
                CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly | CloudFileNtfsAttributes.Hidden,
                CloudFileNtfsAttributes.System | CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Offline |CloudFileNtfsAttributes.NotContentIndexed | CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly | 
                CloudFileNtfsAttributes.Hidden | 
                CloudFileNtfsAttributes.System | 
                CloudFileNtfsAttributes.Archive | 
                CloudFileNtfsAttributes.NotContentIndexed |
                CloudFileNtfsAttributes.NoScrubData
#endif
            };

            for (int i = 0; i < SMBFileAttributes.Length; ++i)
            {
                Test.Info("Testing setting attributes {0} to directories", SMBFileAttributes[i]);
                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i]);

                DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

                var options = new TestExecutionOptions<DMLibDataInfo>();
                options.IsDirectoryTransfer = true;

                options.TransferItemModifier = (fileNode, transferItem) =>
                {
                    transferItem.TransferContext = dirTransferContext;

                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    transferOptions.PreserveSMBAttributes = true;
                    transferItem.Options = transferOptions;
                };

#if DEBUG
                TestHookCallbacks.GetFileAttributesCallback = (path) =>
                {
                    if (path.EndsWith("\\") || path.Substring(0, path.Length - 2).EndsWith(DMLibTestBase.DirName))
                    {
                        if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                        {
                            return FileAttributes.Directory;
                        }
                        else
                        {
                            return Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory);
                        }
                    }
                    else
                    {
                        return Helper.ToFileAttributes(SMBFileAttributes[i]);
                    }
                };

                TestHookCallbacks.SetFileAttributesCallback = (path, attributes) =>
                {
                    if (path.EndsWith("\\") || path.Substring(0, path.Length - 2).EndsWith(DMLibTestBase.DirName))
                    {
                        if (path.Substring(0, path.Length - 7).EndsWith("destroot") || path.Substring(0, path.Length - 6).EndsWith("destroot"))
                        {
                            Test.Assert(attributes == FileAttributes.Directory, "Directory attributes should be expected {0}", attributes);
                        }
                        else
                        {

                            if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                            {
                                Test.Assert(attributes == FileAttributes.Directory, "Directory attributes should be expected {0}", attributes);
                            }
                            else
                            {
                                Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory),
                                    "Directory attributes should be expected {0}", attributes);
                            }
                        }
                    }
                    else
                    {
                        Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i]),
                            "File attributes should be expected {0}", attributes);
                    }
                };
#endif

                try
                {
                    // Execute test case
                    var result = this.ExecuteTestCase(sourceDataInfo, options);

                    options = new TestExecutionOptions<DMLibDataInfo>();
                    options.IsDirectoryTransfer = true;

                    TransferItem item = new TransferItem()
                    {
                        SourceObject = SourceAdaptor.GetTransferObject(string.Empty, sourceDataInfo.RootNode),
                        DestObject = DestAdaptor.GetTransferObject(string.Empty, sourceDataInfo.RootNode),
                        SourceType = DMLibTestContext.SourceType,
                        DestType = DMLibTestContext.DestType,
                        CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                        IsDirectoryTransfer = true,
                        Options = DefaultTransferDirectoryOptions
                    };

                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    transferOptions.PreserveSMBAttributes = true;
                    item.Options = transferOptions;

                    item.TransferContext = new DirectoryTransferContext();
                    item.TransferContext.ShouldOverwriteCallbackAsync = async (s, d) =>
                    {
                        return false;
                    };

                    List<TransferItem> items = new List<TransferItem>();
                    items.Add(item);

                    result = this.RunTransferItems(items, options);

                    VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                    if (DMLibTestContext.DestType == DMLibDataType.Local)
                    {
                        Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, false);
                    }
                    else
                    {
                        Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                    }
                }
                finally
                {
#if DEBUG
                    TestHookCallbacks.GetFileAttributesCallback = null;
                    TestHookCallbacks.SetFileAttributesCallback = null;
#endif
                }
            }
        }


        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestDirectoryPreserveSMBPropertiesResume()
        {
            if (!CrossPlatformHelpers.IsWindows) return;
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly,
                CloudFileNtfsAttributes.Hidden,
#if DEBUG
                CloudFileNtfsAttributes.System,
                CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Normal,
                CloudFileNtfsAttributes.Offline,
                CloudFileNtfsAttributes.NotContentIndexed,
                CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly | CloudFileNtfsAttributes.Hidden,
                CloudFileNtfsAttributes.System | CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Offline |CloudFileNtfsAttributes.NotContentIndexed | CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly |
                CloudFileNtfsAttributes.Hidden |
                CloudFileNtfsAttributes.System |
                CloudFileNtfsAttributes.Archive |
                CloudFileNtfsAttributes.NotContentIndexed |
                CloudFileNtfsAttributes.NoScrubData
#endif
            };

            for (int i = 0; i < SMBFileAttributes.Length; ++i)
            {
                CancellationTokenSource tokenSource = new CancellationTokenSource();
                TransferItem transferItem = null;

                bool IsStreamJournal = random.Next(0, 2) == 0;
                using (Stream journalStream = new MemoryStream())
                {
                    Test.Info("Testing setting attributes {0} to directories", SMBFileAttributes[i]);
                    // Prepare data
                    DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                    GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i]);

                    DirectoryTransferContext dirTransferContext = null;

                    if (IsStreamJournal)
                    {
                        dirTransferContext = new DirectoryTransferContext(journalStream);
                    }
                    else
                    {
                        dirTransferContext = new DirectoryTransferContext();
                    }

                    var progressChecker = new ProgressChecker(14, 14 * 1024);
                    dirTransferContext.ProgressHandler = progressChecker.GetProgressHandler();

                    var options = new TestExecutionOptions<DMLibDataInfo>();
                    options.IsDirectoryTransfer = true;

                    options.TransferItemModifier = (fileNode, item) =>
                    {
                        item.TransferContext = dirTransferContext;

                        dynamic transferOptions = DefaultTransferDirectoryOptions;
                        transferOptions.Recursive = true;
                        transferOptions.PreserveSMBAttributes = true;
                        item.Options = transferOptions;
                        item.CancellationToken = tokenSource.Token;
                        transferItem = item;
                    };

                    TransferCheckpoint checkpoint = null;

                    options.AfterAllItemAdded = () =>
                    {
                        // Wait until there are data transferred
                        progressChecker.DataTransferred.WaitOne();

                        if (!IsStreamJournal)
                        {
                            checkpoint = dirTransferContext.LastCheckpoint;
                        }

                        // Cancel the transfer and store the second checkpoint
                        tokenSource.Cancel();
                    };

#if DEBUG
                    TestHookCallbacks.GetFileAttributesCallback = (path) =>
                    {
                        if (path.EndsWith("\\") || path.Substring(0, path.Length - 2).EndsWith(DMLibTestBase.DirName))
                        {
                            if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                            {
                                return FileAttributes.Directory;
                            }
                            else
                            {
                                return Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory);
                            }
                        }
                        else
                        {
                            return Helper.ToFileAttributes(SMBFileAttributes[i]);
                        }
                    };

                    TestHookCallbacks.SetFileAttributesCallback = (path, attributes) =>
                    {
                        if (path.EndsWith("\\") || path.Substring(0, path.Length - 2).EndsWith(DMLibTestBase.DirName))
                        {
                            if (path.Substring(0, path.Length - 7).EndsWith("destroot") || path.Substring(0, path.Length - 6).EndsWith("destroot"))
                            {
                                Test.Assert(attributes == FileAttributes.Directory, "Directory attributes should be expected {0}", attributes);
                            }
                            else
                            {

                                if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                                {
                                    Test.Assert(attributes == FileAttributes.Directory, "Directory attributes should be expected {0}", attributes);
                                }
                                else
                                {
                                    Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory),
                                        "Directory attributes should be expected {0}", attributes);
                                }
                            }
                        }
                        else
                        {
                            Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i]),
                                "File attributes should be expected {0}", attributes);
                        }
                    };
#endif

                    try
                    {
                        // Execute test case
                        var result = this.ExecuteTestCase(sourceDataInfo, options);

                        Test.Assert(result.Exceptions.Count == 1, "Verify job is cancelled");
                        Exception exception = result.Exceptions[0];
                        Helper.VerifyCancelException(exception);

                        TransferItem resumeItem = transferItem.Clone();
                        DirectoryTransferContext resumeContext = null;
                        journalStream.Position = 0;
                        if (IsStreamJournal)
                        {
                            resumeContext = new DirectoryTransferContext(journalStream);
                        }
                        else
                        {
                            resumeContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(checkpoint));
                        }

                        resumeItem.TransferContext = resumeContext;

                        result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());
                        VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                        options = new TestExecutionOptions<DMLibDataInfo>();
                        options.IsDirectoryTransfer = true;

                        TransferItem item = new TransferItem()
                        {
                            SourceObject = SourceAdaptor.GetTransferObject(string.Empty, sourceDataInfo.RootNode),
                            DestObject = DestAdaptor.GetTransferObject(string.Empty, sourceDataInfo.RootNode),
                            SourceType = DMLibTestContext.SourceType,
                            DestType = DMLibTestContext.DestType,
                            CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                            IsDirectoryTransfer = true,
                            Options = DefaultTransferDirectoryOptions
                        };

                        dynamic transferOptions = DefaultTransferDirectoryOptions;
                        transferOptions.Recursive = true;
                        transferOptions.PreserveSMBAttributes = true;
                        item.Options = transferOptions;

                        item.TransferContext = new DirectoryTransferContext();
                        item.TransferContext.ShouldOverwriteCallbackAsync = async (s, d) =>
                        {
                            return false;
                        };

                        List<TransferItem> items = new List<TransferItem>();
                        items.Add(item);

                        result = this.RunTransferItems(items, options);

                        VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                        if (DMLibTestContext.DestType == DMLibDataType.Local)
                        {
                            Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, false);
                        }
                        else
                        {
                            Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                        }
                    }
                    finally
                    {
#if DEBUG
                        TestHookCallbacks.GetFileAttributesCallback = null;
                        TestHookCallbacks.SetFileAttributesCallback = null;
#endif
                    }
                }
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestPreserveSMBProperties()
        {
            if (!CrossPlatformHelpers.IsWindows) return;
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly,
                CloudFileNtfsAttributes.Hidden,
#if DEBUG
                CloudFileNtfsAttributes.System,
                CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Normal,
                CloudFileNtfsAttributes.Temporary,
                CloudFileNtfsAttributes.Offline,
                CloudFileNtfsAttributes.NotContentIndexed,
                CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly | CloudFileNtfsAttributes.Hidden,
                CloudFileNtfsAttributes.System | CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Temporary | CloudFileNtfsAttributes.Offline,
                CloudFileNtfsAttributes.NotContentIndexed | CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly |
                CloudFileNtfsAttributes.Hidden |
                CloudFileNtfsAttributes.System |
                CloudFileNtfsAttributes.Archive |
                CloudFileNtfsAttributes.Temporary |
                CloudFileNtfsAttributes.NotContentIndexed |
                CloudFileNtfsAttributes.NoScrubData
#endif
            };

            try
            {
                for (int i = 0; i < SMBFileAttributes.Length; ++i)
                {
#if DEBUG
                    TestHookCallbacks.GetFileAttributesCallback = (path) =>
                    {
                        if (path.EndsWith("\\") || path.EndsWith(DMLibTestBase.DirName))
                        {
                            if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                            {
                                return FileAttributes.Directory;
                            }
                            else
                            {
                                return Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory);
                            }
                        }
                        else
                        {
                            return Helper.ToFileAttributes(SMBFileAttributes[i]);
                        }
                    };

                    TestHookCallbacks.SetFileAttributesCallback = (path, attributes) =>
                    {
                        if (path.EndsWith("\\") || path.EndsWith(DMLibTestBase.DirName))
                        {
                            if (SMBFileAttributes[i] == CloudFileNtfsAttributes.Normal)
                            {
                                Test.Assert(attributes == FileAttributes.Directory, "Directory attributes should be expected {0}", attributes);
                            }
                            else
                            {
                                Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i] | CloudFileNtfsAttributes.Directory),
                                    "Directory attributes should be expected {0}", attributes);
                            }
                        }
                        else
                        {
                            Test.Assert(attributes == Helper.ToFileAttributes(SMBFileAttributes[i]),
                                "File attributes should be expected {0}", attributes);
                        }
                    };
#endif

                    // Prepare data
                    DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                    FileNode fileNode = new FileNode(DMLibTestBase.FileName);
                    fileNode.SizeInByte = 1024;
                    fileNode.SMBAttributes = SMBFileAttributes[i];
                    sourceDataInfo.RootNode.AddFileNode(fileNode);

                    SingleTransferContext transferContext = new SingleTransferContext();

                    var options = new TestExecutionOptions<DMLibDataInfo>();

                    options.TransferItemModifier = (fileNodeVar, transferItem) =>
                    {
                        transferItem.TransferContext = transferContext;

                        dynamic transferOptions = DefaultTransferOptions;
                        transferOptions.PreserveSMBAttributes = true;
                        transferItem.Options = transferOptions;
                    };

                    // Execute test case
                    var result = this.ExecuteTestCase(sourceDataInfo, options);

                    VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                    if (DMLibTestContext.DestType == DMLibDataType.Local)
                    {
                        Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, false);
                    }
                    else
                    {
                        Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                    }
                }
            }
            finally
            {
#if DEBUG
                TestHookCallbacks.GetFileAttributesCallback = null;
                TestHookCallbacks.SetFileAttributesCallback = null;
#endif
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudFile)]
        public void TestDirectoryMeta()
        {
            // Prepare data
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
            GenerateDirNodeWithMetadata(sourceDataInfo.RootNode, 2);

            DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = dirTransferContext;

                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            // Execute test case
            var result = this.ExecuteTestCase(sourceDataInfo, options);
            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local, DMLibCopyMethod.SyncCopy)]
        public void TestNotsupportedPlatform()
        {
            Exception exception = null;
            try
            {
                DownloadDirectoryOptions downloadDirectoryOptions = new DownloadDirectoryOptions()
                {
                    PreserveSMBAttributes = true,
                };
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                UploadDirectoryOptions uploadDirectoryOptions = new UploadDirectoryOptions()
                {
                    PreserveSMBAttributes = true,
                };
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                DownloadOptions downloadOptions = new DownloadOptions()
                {
                    PreserveSMBAttributes = true,
                };
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                UploadOptions uploadOptions = new UploadOptions()
                {
                    PreserveSMBAttributes = true,
                };
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);
        }

        private static void ValidateExceptionForParemterSetting(Exception exception)
        {
            if (!CrossPlatformHelpers.IsWindows)
            {
                if (null == exception || !(exception is PlatformNotSupportedException))
                {
                    Test.Error("PlatformNotSupportedException expected for Non-Windows platform");
                }
            }
            else
            {
                if (null != exception)
                {
                    Test.Error("Should no exception for Windows platform");
                }
            }
        }

        private void GenerateDirNodeWithMetadata(
           DirNode parent,
           int dirLevel)
        {
            FileNode fileNode = new FileNode(DMLibTestBase.FileName + "_0");
            parent.AddFileNode(fileNode);

            fileNode = new FileNode(DMLibTestBase.FileName + "_1");
            parent.AddFileNode(fileNode);

            if (dirLevel <= 0)
            {
                return;
            }

            --dirLevel;
            DirNode dirNode = new DirNode(DMLibTestBase.DirName + "_0");
            dirNode.Metadata = new Dictionary<string, string>();
            dirNode.Metadata.Add("Name0", "Value0");
            dirNode.Metadata.Add("Name1", "Value1");
            this.GenerateDirNodeWithMetadata(dirNode, dirLevel);
            parent.AddDirNode(dirNode);

            dirNode = new DirNode(DMLibTestBase.DirName + "_1");
            dirNode.Metadata = new Dictionary<string, string>();
            dirNode.Metadata.Add("Name0", "Value0");
            dirNode.Metadata.Add("Name1", "Value1");
            this.GenerateDirNodeWithMetadata(dirNode, dirLevel);
            parent.AddDirNode(dirNode);
        }

        private void GenerateDirNodeWithAttributes(
            DirNode parent,
            int dirLevel,
            CloudFileNtfsAttributes smbAttributes)
        {
            FileNode fileNode = new FileNode(DMLibTestBase.FileName + "_0");
            fileNode.SizeInByte = 1024;
            fileNode.SMBAttributes = smbAttributes;
            parent.AddFileNode(fileNode);

            fileNode = new FileNode(DMLibTestBase.FileName + "_1");
            fileNode.SizeInByte = 1024;
            fileNode.SMBAttributes = smbAttributes;
            parent.AddFileNode(fileNode);

            if (dirLevel <= 0)
            {
                return;
            }

            --dirLevel;
            DirNode dirNode = new DirNode(DMLibTestBase.DirName + "_0");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes);
            if (smbAttributes == CloudFileNtfsAttributes.Normal)
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory;
            }
            else
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory | smbAttributes;
            }
            parent.AddDirNode(dirNode);

            dirNode = new DirNode(DMLibTestBase.DirName + "_1");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes);
            if (smbAttributes == CloudFileNtfsAttributes.Normal)
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory;
            }
            else
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory | smbAttributes;
            }
            parent.AddDirNode(dirNode);
        }
    }
}
