//------------------------------------------------------------------------------
// <copyright file="LongFilePathTest.cs" company="Microsoft">
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
    using System.IO;
    using System.Collections.Generic;
#if DNXCORE50
    using Xunit;

    [Collection(Collections.Global)]
    public class LongFilePathTest : DMLibTestBase, IClassFixture<AllTransferDirectionFixture>, IDisposable
    {
        public LongFilePathTest()
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
#else
    [MultiDirectionTestClass]
    public class LongFilePathTest : DMLibTestBase
    {
#endif
        #region Initialization and cleanup methods
        private string shortFileName = new string('a', 124);
        private string longFileName = new string('a', 1024);

        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: LongFilePathTest");
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

            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibTestContext.DestType);
            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            // string destFileName = new string('t', 124);
            string destFileName = sourceFileName;
            DMLibDataHelper.AddOneFile(destDataInfo.RootNode, destFileName, 5 * 1024);
            FileNode destFileNode = destDataInfo.RootNode.GetFileNode(destFileName);

            destAdaptor.Cleanup();
            destAdaptor.CreateIfNotExists();
            // destAdaptor.GenerateData(destDataInfo);


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


            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
            };

            var result = this.RunTransferItems(new List<TransferItem> { longFilePathItem }, options);
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathUpload()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            string sourceFileName = GetTransferString(DMLibTestContext.SourceType, DMLibTestContext.DestType, DMLibTestContext.IsAsync);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, sourceFileName, 5 * 1024);
            FileNode sourceFileNode = sourceDataInfo.RootNode.GetFileNode(sourceFileName);

            DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(DMLibTestContext.SourceType);
            sourceAdaptor.Cleanup();
            sourceAdaptor.CreateIfNotExists();
            sourceAdaptor.GenerateData(sourceDataInfo);

            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibTestContext.DestType);
            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            // string destFileName = new string('t', 124);
            string destFileName = sourceFileName;
            FileNode destFileNode = destDataInfo.RootNode.GetFileNode(destFileName);

            TransferItem longFilePathItem = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceFileNode),
                DestObject = destAdaptor.GetTransferObject(destDataInfo.RootPath, destFileNode),
                IsDirectoryTransfer = false,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                IsServiceCopy = DMLibTestContext.IsAsync,
                TransferContext = new SingleTransferContext()
            };


            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
            };
            var result = this.RunTransferItems(new List<TransferItem> { longFilePathItem }, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.BVT)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongPathFileStreamWrite()
        {
            string destFileName = this.shortFileName;
            Stream outputStream = new LongPathFileStream(
                destFileName,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None);
            byte[] data = new byte[byte.MaxValue];
            for (byte i = 0; i < byte.MaxValue; ++i)
                data[i] = i;
            outputStream.Write(data, 0, byte.MaxValue);
            outputStream.SetLength(byte.MaxValue);
#if DNXCORE50
            outputStream.Dispose();
#else
            outputStream.Close();
#endif
        }

        [TestCategory(Tag.BVT)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongPathFileStreamRead()
        {
            this.LongPathFileStreamWrite();

            string sourceFileName = this.shortFileName;
            Stream inputStream = new LongPathFileStream(
                sourceFileName,
                FileMode.Open,
                FileAccess.Read,
                FileShare.None);
            byte[] data = new byte[byte.MaxValue];
            inputStream.Read(data, 0, byte.MaxValue);
            for (byte i = 0; i < byte.MaxValue; ++i)
                Test.Assert(data[i] == i, "Data verification");
#if DNXCORE50
            inputStream.Dispose();
#else
            inputStream.Close();
#endif
        }

        private static string GetTransferString(DMLibDataType sourceType, DMLibDataType destType, bool isAsync)
        {
            return sourceType.ToString() + destType.ToString() + (isAsync ? "async" : "");
        }
    }
}
