//------------------------------------------------------------------------------
// <copyright file="DelimiterTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class DelimiterTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public DelimiterTest()
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
            Test.Info("Class Initialize: DelimiterTest");
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
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile | DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile, copymethod: DMLibCopyMethod.SyncCopy)]
        public void TestNormalDelimiter()
        {
            char delimiter = Helper.GenerateRandomDelimiter();
            TestDelimiter(delimiter);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile | DMLibDataType.Local)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile, copymethod: DMLibCopyMethod.SyncCopy)]
        public void TestSpecialDelimiter()
        {
            string specialChars = "~`!@#$%()-_+={}[];‘,.^& ";
            TestDelimiter(specialChars[random.Next(0, specialChars.Length)]);
        }

        private void TestDelimiter(char delimiter)
        {
            Test.Info("Test delimiter: {0}", delimiter);
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            string fileName = DMLibTestBase.FolderName + delimiter + DMLibTestBase.FolderName + delimiter + DMLibTestBase.FileName;
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, fileName, 1);

            TransferContext context = new DirectoryTransferContext();
            context.FileFailed += (sender, e) =>
            {
                Test.Info(e.Exception.StackTrace);
            };

            TestExecutionOptions<DMLibDataInfo> options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (node, item) =>
                {
                    dynamic dirOptions = DefaultTransferDirectoryOptions;
                    dirOptions.Recursive = true;
                    dirOptions.Delimiter = delimiter;
                    item.Options = dirOptions;
                    item.TransferContext = context;
                }
            };

            TestResult<DMLibDataInfo> result = this.ExecuteTestCase(sourceDataInfo, options);

            DMLibDataInfo expectedDataInfo = new DMLibDataInfo(string.Empty);
            DirNode dirNode1 = new DirNode(FolderName);
            DirNode dirNode2 = new DirNode(FolderName);
            FileNode fileNode = sourceDataInfo.RootNode.GetFileNode(fileName).Clone(DMLibTestBase.FileName);

            dirNode2.AddFileNode(fileNode);
            dirNode1.AddDirNode(dirNode2);
            expectedDataInfo.RootNode.AddDirNode(dirNode1);

            VerificationHelper.VerifySingleTransferStatus(result, 1, 0, 0, null);
            VerificationHelper.VerifyTransferSucceed(result, expectedDataInfo);
        }
    }
}
