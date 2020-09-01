using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DMLibTestCodeGen;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MS.Test.Common.MsTestLib;

namespace DMLibTest.Cases
{
    [MultiDirectionTestClass]
    public class EncryptionScopeTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public EncryptionScopeTest()
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
            Test.Info("Class Initialize: EncryptionScopeTest");
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
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.SyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.SyncCopy)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudBlob)]
        [DMLibTestMethod(DMLibDataType.Stream, DMLibDataType.CloudBlob)]
        public void TestSingleBlobWithEncryptionScope()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, DMLibTestBase.FileName, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;
                transferOptions.EncryptionScope = Test.Data.Get(DMLibTestConstants.DestEncryptionScope);

                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

            string destEncryptionScope = result.DataInfo.RootNode.FileNodes.First().EncryptionScope;
            Test.Assert(string.Equals(destEncryptionScope, Test.Data.Get(DMLibTestConstants.DestEncryptionScope)),
                "Encryption scope name in destination should be expected {0} == {1}", destEncryptionScope, Test.Data.Get(DMLibTestConstants.DestEncryptionScope));
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.SyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.SyncCopy)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudBlob)]
        public void TestBlobDirectoryWithEncryptionScope()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddMultipleFilesNormalSize(sourceDataInfo.RootNode, DMLibTestBase.FileName);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.EncryptionScope = Test.Data.Get(DMLibTestConstants.DestEncryptionScope);
                transferOptions.Recursive = true;

                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

            foreach (var fileNode in result.DataInfo.RootNode.EnumerateFileNodesRecursively())
            {
                string destEncryptionScope = result.DataInfo.RootNode.FileNodes.First().EncryptionScope;
                Test.Assert(string.Equals(destEncryptionScope, Test.Data.Get(DMLibTestConstants.DestEncryptionScope)),
                    "Encryption scope name in destination should be expected {0} == {1}", destEncryptionScope, Test.Data.Get(DMLibTestConstants.DestEncryptionScope));
            }
        }
    }
}
