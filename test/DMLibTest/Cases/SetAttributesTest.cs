namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using System.Collections.Generic;
    [MultiDirectionTestClass]
    public class SetAttributesTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public SetAttributesTest()
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
            Test.Info("Class Initialize: SetAttributeTest");
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

#if DNXCORE50

        // It doesn't work in .Net Core to get content type value without subtype.
        // Content type should always include a subtype, here change test cases according to it.
        // Should also test the feature to set content type without subtype when .Net Core fixes its issue.
        private const string TestContentType = "newtype/subtype";
#else
        private const string TestContentType = "newtype";
#endif

        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        public void TestDirectorySetAttributesToLocal()
        {
            TestSetAttributesToLocal(true);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.Local)]
        public void TestSetAttributesToLocal()
        {
            TestSetAttributesToLocal(false);
        }

        private void TestSetAttributesToLocal(bool IsDirectoryTransfer)
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB* 1024L,
            };

            sourceDataInfo.RootNode.AddFileNode(fileNode);

            TransferContext context;
            
            if (IsDirectoryTransfer)
            {
                context = new DirectoryTransferContext()
                {
                    SetAttributesCallback = (destObj) =>
                    {
                        Test.Error("SetAttributes callback should not be invoked when destination is local");
                    }
                };
            }
            else
            {
                context = new SingleTransferContext()
                {
                    SetAttributesCallback = (destObj) =>
                    {
                        Test.Error("SetAttributes callback should not be invoked when destination is local");
                    }
                };
            }

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = IsDirectoryTransfer ? DefaultTransferDirectoryOptions : DefaultTransferOptions;

                if (IsDirectoryTransfer)
                {
                    transferOptions.Recursive = true;
                }

                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };

            options.IsDirectoryTransfer = IsDirectoryTransfer;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudDest)]
        public void TestSetAttributes()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            if (DMLibTestContext.SourceType != DMLibDataType.Local)
            {
                metadata.Add("foo", "bar");
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
                Metadata = metadata
            };

            sourceDataInfo.RootNode.AddFileNode(fileNode);

            TransferContext context = new SingleTransferContext()
            {
                SetAttributesCallback = (destObj) =>
                {
                    dynamic destCloudObj = destObj;

                    destCloudObj.Properties.ContentType = SetAttributesTest.TestContentType;

                    destCloudObj.Metadata.Add("aa", "bb");
                }
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;
                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            fileNode.Metadata.Add("aa", "bb");

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

            FileNode destFileNode = result.DataInfo.RootNode.GetFileNode(DMLibTestBase.FileName);
            Test.Assert(TestContentType.Equals(destFileNode.ContentType), "Verify content type: {0}, expected {1}", destFileNode.ContentType, TestContentType);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudDest)]
        public void TestDirectorySetAttributes()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            if (DMLibTestContext.SourceType != DMLibDataType.Local)
            {
                metadata.Add("foo", "bar");
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            for (int i = 0; i < 3; ++i)
            {
                FileNode fileNode = new FileNode(DMLibTestBase.FileName + i)
                {
                    SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
                    Metadata = metadata
                };

                sourceDataInfo.RootNode.AddFileNode(fileNode);
            }

            DirectoryTransferContext context = new DirectoryTransferContext()
            {
                SetAttributesCallback = (destObj) =>
                {
                    dynamic destCloudObj = destObj;

                    destCloudObj.Properties.ContentType = SetAttributesTest.TestContentType;

                    destCloudObj.Metadata.Add("aa", "bb");
                }
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };
            options.IsDirectoryTransfer = true;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            foreach (FileNode fileNode in sourceDataInfo.EnumerateFileNodes())
            {
                fileNode.Metadata.Add("aa", "bb");
            }

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
            VerificationHelper.VerifySingleTransferStatus(result, 3, 0, 0, 3 * DMLibTestBase.FileSizeInKB * 1024L);

            foreach(FileNode destFileNode in result.DataInfo.EnumerateFileNodes())
            {
                Test.Assert(TestContentType.Equals(destFileNode.ContentType), "Verify content type: {0}, expected {1}", destFileNode.ContentType, TestContentType);
            }
        }
    }
}
