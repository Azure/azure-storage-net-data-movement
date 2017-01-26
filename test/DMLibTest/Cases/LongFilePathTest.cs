//------------------------------------------------------------------------------
// <copyright file="SnapshotTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using System;
    using System.Collections.Generic;

    [MultiDirectionTestClass]
    public class LongFilePathTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods
#if DNXCORE50
        public SnapshotTest()
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
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongFilePathDownload()
        {
            
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            string sourceFileName = GetTransferString(DMLibTestContext.SourceType, DMLibTestContext.DestType, DMLibTestContext.IsAsync);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, sourceFileName, 5 * 1024);
            FileNode sourceFileNode = sourceDataInfo.RootNode.GetFileNode(sourceFileName);

            DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(DMLibTestContext.SourceType);
            sourceAdaptor.Cleanup();
            sourceAdaptor.CreateIfNotExists();
            sourceAdaptor.GenerateData(sourceDataInfo);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            string destFileName = new string('t', 102400);
            DMLibDataHelper.AddOneFile(destDataInfo.RootNode, destFileName, 5 * 1024);
            FileNode destFileNode = destDataInfo.RootNode.GetFileNode(destFileName);

            TransferItem longFilePathItem = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceFileNode),
                DestObject = DestAdaptor.GetTransferObject(destDataInfo.RootPath, destFileNode),
                IsDirectoryTransfer = false,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                IsServiceCopy = DMLibTestContext.IsAsync,
                TransferContext = new SingleTransferContext()
            };

            var result = this.RunTransferItems(new List<TransferItem> { longFilePathItem }, new TestExecutionOptions<DMLibDataInfo>());
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");
        }

        private static string GetTransferString(DMLibDataType sourceType, DMLibDataType destType, bool isAsync)
        {
            return sourceType.ToString() + destType.ToString() + (isAsync ? "async" : "");
        }
    }
}
