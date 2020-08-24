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
                GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i], null);

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
                    GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i], null);

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
                        bool gotProgress = progressChecker.DataTransferred.WaitOne(60000);

                        Test.Assert(gotProgress, "Should got progress");

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
        [DMLibTestMethod(DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy)]
        public void TestCopySMBPropertiesACL()
        {
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly,
                CloudFileNtfsAttributes.Hidden,

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
            };

            string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";

            for (int i = 0; i < SMBFileAttributes.Length; ++i)
            {
                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                FileNode fileNode = new FileNode(DMLibTestBase.FileName);
                fileNode.SizeInByte = 1024;
                fileNode.SMBAttributes = SMBFileAttributes[i];
                fileNode.PortableSDDL = sampleSDDL;
                sourceDataInfo.RootNode.AddFileNode(fileNode);

                SingleTransferContext transferContext = new SingleTransferContext();

                var options = new TestExecutionOptions<DMLibDataInfo>();

                options.TransferItemModifier = (fileNodeVar, transferItem) =>
                {
                    transferItem.TransferContext = transferContext;

                    dynamic transferOptions = DefaultTransferOptions;
                    transferOptions.PreserveSMBAttributes = true;
                    transferOptions.PreserveSMBPermissions = true;
                    transferItem.Options = transferOptions;
                };

                // Execute test case
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                Helper.CompareSMBPermissions(
                    sourceDataInfo.RootNode, 
                    result.DataInfo.RootNode, 
                    PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL);
            }
        }


        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy)]
        public void TestCopyDirectorySMBPropertiesACL()
        {
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly | CloudFileNtfsAttributes.Hidden,
                CloudFileNtfsAttributes.System | CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Offline |CloudFileNtfsAttributes.NotContentIndexed | CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly |
                CloudFileNtfsAttributes.Hidden |
                CloudFileNtfsAttributes.System |
                CloudFileNtfsAttributes.Archive |
                CloudFileNtfsAttributes.NotContentIndexed |
                CloudFileNtfsAttributes.NoScrubData
            };

            string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";

            for (int i = 0; i < SMBFileAttributes.Length; ++i)
            {
                Test.Info("Testing setting attributes {0} to directories", SMBFileAttributes[i]);
                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i], sampleSDDL);

                DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

                var options = new TestExecutionOptions<DMLibDataInfo>();
                options.IsDirectoryTransfer = true;

                options.TransferItemModifier = (fileNode, transferItem) =>
                {
                    transferItem.TransferContext = dirTransferContext;

                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    transferOptions.PreserveSMBAttributes = true;
                    transferOptions.PreserveSMBPermissions = true;
                    transferItem.Options = transferOptions;
                };

                // Execute test case
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                Helper.CompareSMBPermissions(
                    sourceDataInfo.RootNode,
                    result.DataInfo.RootNode,
                    PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL);
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy)]
        public void TestCopyDirectorySMBPropertiesACLResume()
        {
            CloudFileNtfsAttributes[] SMBFileAttributes = {
                CloudFileNtfsAttributes.ReadOnly | CloudFileNtfsAttributes.Hidden,
                CloudFileNtfsAttributes.System | CloudFileNtfsAttributes.Archive,
                CloudFileNtfsAttributes.Offline |CloudFileNtfsAttributes.NotContentIndexed | CloudFileNtfsAttributes.NoScrubData,

                CloudFileNtfsAttributes.ReadOnly |
                CloudFileNtfsAttributes.Hidden |
                CloudFileNtfsAttributes.System |
                CloudFileNtfsAttributes.Archive |
                CloudFileNtfsAttributes.NotContentIndexed |
                CloudFileNtfsAttributes.NoScrubData
            };

            string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";

            for (int i = 0; i < SMBFileAttributes.Length; ++i)
            {
                Test.Info("Testing setting attributes {0} to directories", SMBFileAttributes[i]);
                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, SMBFileAttributes[i], sampleSDDL);

                CancellationTokenSource tokenSource = new CancellationTokenSource();
                TransferItem transferItem = null;

                bool IsStreamJournal = random.Next(0, 2) == 0;
                using (Stream journalStream = new MemoryStream())
                {
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
                        transferOptions.PreserveSMBPermissions = true;
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

                    Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
                    Helper.CompareSMBPermissions(
                        sourceDataInfo.RootNode,
                        result.DataInfo.RootNode,
                        PreserveSMBPermissions.Owner | PreserveSMBPermissions.Group | PreserveSMBPermissions.DACL | PreserveSMBPermissions.SACL);
                }
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy)]
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
            DownloadDirectoryOptions downloadDirectoryOptions = new DownloadDirectoryOptions();
            try
            {
                downloadDirectoryOptions.PreserveSMBAttributes = true;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                downloadDirectoryOptions.PreserveSMBPermissions = PreserveSMBPermissions.Owner;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            UploadDirectoryOptions uploadDirectoryOptions = new UploadDirectoryOptions();
            try
            {
                uploadDirectoryOptions.PreserveSMBAttributes = true;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                uploadDirectoryOptions.PreserveSMBPermissions = PreserveSMBPermissions.Owner;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            DownloadOptions downloadOptions = new DownloadOptions();
            try
            {
                downloadOptions.PreserveSMBAttributes = true;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            try
            {
                downloadOptions.PreserveSMBPermissions = PreserveSMBPermissions.Owner;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            exception = null;
            UploadOptions uploadOptions = new UploadOptions();
            try
            {
                uploadOptions.PreserveSMBAttributes = true;
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ValidateExceptionForParemterSetting(exception);

            try
            {
                uploadOptions.PreserveSMBPermissions = PreserveSMBPermissions.Owner;
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

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestDirectoryPreserveSMBPermissions()
        {
            if (!CrossPlatformHelpers.IsWindows) return;

            // Prepare data
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
            string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";
            GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, null, sampleSDDL);

            DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            PreserveSMBPermissions preserveSMBPermissions =
                PreserveSMBPermissions.Owner 
                | PreserveSMBPermissions.Group 
                | PreserveSMBPermissions.DACL;

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = dirTransferContext;

                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferOptions.PreserveSMBPermissions = preserveSMBPermissions;
                transferItem.Options = transferOptions;
            };

#if DEBUG
            TestHookCallbacks.UnderTesting = true;
            TestHookCallbacks.GetFilePermissionsCallback = (path, SMBPermissionType) =>
            {
                Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                return sampleSDDL;
            };

            TestHookCallbacks.SetFilePermissionsCallback = (path, portableSDDL, SMBPermissionType) =>
            {
                Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                Test.Assert(portableSDDL.StartsWith(sampleSDDL),
                    "The SDDL value should be expected.");
            };
#endif

            try
            {
                // Execute test case
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
                {
                    Helper.CompareSMBPermissions(sourceDataInfo.RootNode, result.DataInfo.RootNode, preserveSMBPermissions);
                }
            }
            finally
            {
#if DEBUG
                TestHookCallbacks.UnderTesting = false;
                TestHookCallbacks.GetFilePermissionsCallback = null;
                TestHookCallbacks.SetFilePermissionsCallback = null;
#endif
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestPreserveSMBPermissions()
        {
            if (!CrossPlatformHelpers.IsWindows) return;

            try
            {
                PreserveSMBPermissions preserveSMBPermissions = 
                    PreserveSMBPermissions.Owner 
                    | PreserveSMBPermissions.Group 
                    | PreserveSMBPermissions.DACL;
                string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";

#if DEBUG
                TestHookCallbacks.UnderTesting = true;
                TestHookCallbacks.GetFilePermissionsCallback = (path, SMBPermissionType) =>
                {
                    Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                    return sampleSDDL;
                };

                TestHookCallbacks.SetFilePermissionsCallback = (path, portableSDDL, SMBPermissionType) =>
                {
                    Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                    Test.Assert(portableSDDL.StartsWith(sampleSDDL),
                        "The SDDL value should be expected.");
                };
#endif

                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                    FileNode fileNode = new FileNode(DMLibTestBase.FileName);
                    fileNode.SizeInByte = 1024;
                    fileNode.PortableSDDL = sampleSDDL;
                    sourceDataInfo.RootNode.AddFileNode(fileNode);

                    SingleTransferContext transferContext = new SingleTransferContext();

                    var options = new TestExecutionOptions<DMLibDataInfo>();

                    options.TransferItemModifier = (fileNodeVar, transferItem) =>
                    {
                        transferItem.TransferContext = transferContext;

                        dynamic transferOptions = DefaultTransferOptions;
                        transferOptions.PreserveSMBPermissions = preserveSMBPermissions;
                        transferItem.Options = transferOptions;
                    };

                    // Execute test case
                    var result = this.ExecuteTestCase(sourceDataInfo, options);

                    VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                    if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
                    {
                        Helper.CompareSMBPermissions(sourceDataInfo.RootNode, result.DataInfo.RootNode, preserveSMBPermissions);
                    }
                }
            finally
            {
#if DEBUG
                TestHookCallbacks.UnderTesting = false;
                TestHookCallbacks.GetFilePermissionsCallback = null;
                TestHookCallbacks.SetFilePermissionsCallback = null;
#endif
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestDirectoryPreserveSMBPermissionsResume()
        {
            if (!CrossPlatformHelpers.IsWindows) return;

            PreserveSMBPermissions preserveSMBPermissions = 
                PreserveSMBPermissions.Owner 
                | PreserveSMBPermissions.Group 
                | PreserveSMBPermissions.DACL;

            string sampleSDDL = "O:S-1-5-21-2146773085-903363285-719344707-1375029G:S-1-5-21-2146773085-903363285-719344707-513D:(A;ID;FA;;;BA)(A;OICIIOID;GA;;;BA)(A;ID;FA;;;SY)(A;OICIIOID;GA;;;SY)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;ID;0x1200a9;;;BU)(A;OICIIOID;GXGR;;;BU)";
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            TransferItem transferItem = null;

            bool IsStreamJournal = random.Next(0, 2) == 0;
            using (Stream journalStream = new MemoryStream())
            {
                // Prepare data
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
                GenerateDirNodeWithAttributes(sourceDataInfo.RootNode, 2, null, sampleSDDL);

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
                    transferOptions.PreserveSMBPermissions = preserveSMBPermissions;
                    item.Options = transferOptions;
                    item.CancellationToken = tokenSource.Token;
                    transferItem = item;
                };

                TransferCheckpoint checkpoint = null;

                options.AfterAllItemAdded = () =>
                {
                    // Wait until there are data transferred
                    bool gotProgress = progressChecker.DataTransferred.WaitOne(60000);

                    Test.Assert(gotProgress, "Should got progress");

                    if (!IsStreamJournal)
                    {
                        checkpoint = dirTransferContext.LastCheckpoint;
                    }

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                };

#if DEBUG
                TestHookCallbacks.UnderTesting = true;
                TestHookCallbacks.GetFilePermissionsCallback = (path, SMBPermissionType) =>
                {
                    Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                    return sampleSDDL;
                };

                TestHookCallbacks.SetFilePermissionsCallback = (path, portableSDDL, SMBPermissionType) =>
                {
                    Test.Assert(SMBPermissionType == preserveSMBPermissions, "The SMB permission type should be expected.");
                    Test.Assert(portableSDDL.StartsWith(sampleSDDL),
                        "The SDDL value should be expected.");
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

                    if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
                    {
                        Helper.CompareSMBPermissions(sourceDataInfo.RootNode, result.DataInfo.RootNode, preserveSMBPermissions);
                    }
                }
                finally
                {
#if DEBUG
                    TestHookCallbacks.UnderTesting = false;
                    TestHookCallbacks.GetFilePermissionsCallback = null;
                    TestHookCallbacks.SetFilePermissionsCallback = null;
#endif
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
            CloudFileNtfsAttributes? smbAttributes,
            string permissions)
        {
            FileNode fileNode = new FileNode(DMLibTestBase.FileName + "_0");
            fileNode.SizeInByte = 1024;
            fileNode.SMBAttributes = smbAttributes;
            fileNode.PortableSDDL = permissions;
            parent.AddFileNode(fileNode);

            fileNode = new FileNode(DMLibTestBase.FileName + "_1");
            fileNode.SizeInByte = 1024;
            fileNode.SMBAttributes = smbAttributes;
            fileNode.PortableSDDL = permissions;
            parent.AddFileNode(fileNode);

            if (dirLevel <= 0)
            {
                return;
            }

            --dirLevel;
            DirNode dirNode = new DirNode(DMLibTestBase.DirName + "_0");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes, permissions);
            if (smbAttributes == CloudFileNtfsAttributes.Normal)
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory;
            }
            else
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory | smbAttributes;
            }
            dirNode.PortableSDDL = permissions;
            parent.AddDirNode(dirNode);

            dirNode = new DirNode(DMLibTestBase.DirName + "_1");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes, permissions);
            if (smbAttributes == CloudFileNtfsAttributes.Normal)
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory;
            }
            else
            {
                dirNode.SMBAttributes = CloudFileNtfsAttributes.Directory | smbAttributes;
            }
            dirNode.PortableSDDL = permissions;
            parent.AddDirNode(dirNode);
        }
    }
}
