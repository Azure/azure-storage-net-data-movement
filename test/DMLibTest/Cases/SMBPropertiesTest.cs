using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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

#if DEBUG
        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void TestPreserveSMBProperties()
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

            try
            {
                for (int i = 0; i < SMBFileAttributes.Length; ++i)
                {
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
                TestHookCallbacks.GetFileAttributesCallback = null;
                TestHookCallbacks.SetFileAttributesCallback = null;
            }
        }
#endif

        private void GenerateDirNodeWithAttributes(
            DirNode parent, 
            int dirLevel, 
            CloudFileNtfsAttributes smbAttributes)
        {
            if (smbAttributes == CloudFileNtfsAttributes.Normal)
            {
                parent.SMBAttributes = CloudFileNtfsAttributes.Directory;
            }
            else
            {
                parent.SMBAttributes = CloudFileNtfsAttributes.Directory | smbAttributes;
            }

            FileNode fileNode = new FileNode(DMLibTestBase.FileName + "_0");
            fileNode.SMBAttributes = smbAttributes;
            parent.AddFileNode(fileNode);

            fileNode = new FileNode(DMLibTestBase.FileName + "_1");
            fileNode.SMBAttributes = smbAttributes;
            parent.AddFileNode(fileNode);

            if (dirLevel <= 0)
            {
                return;
            }

            --dirLevel;
            DirNode dirNode = new DirNode(DMLibTestBase.DirName + "_0");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes);
            parent.AddDirNode(dirNode);

            dirNode = new DirNode(DMLibTestBase.DirName + "_1");
            this.GenerateDirNodeWithAttributes(dirNode, dirLevel, smbAttributes);
            parent.AddDirNode(dirNode);
        }
    }
}
