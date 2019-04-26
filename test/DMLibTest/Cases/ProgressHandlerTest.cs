//------------------------------------------------------------------------------
// <copyright file="ProgressHandlerTest.cs" company="Microsoft">
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
    public class ProgressHandlerTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public ProgressHandlerTest()
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
            Test.Info("Class Initialize: ProgressHandlerTest");
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
        [DMLibTestMethodSet(DMLibTestMethodSet.AllValidDirection)]
        public void TestProgressHandlerTest()
        {
            long fileSize = 10 * 1024 * 1024;

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSize);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                TransferContext transferContext = new SingleTransferContext();
                ProgressChecker progressChecker = new ProgressChecker(1, fileNode.SizeInByte);
                transferContext.ProgressHandler = progressChecker.GetProgressHandler();
                transferItem.TransferContext = transferContext;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }
    }
}
