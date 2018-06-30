//------------------------------------------------------------------------------
// <copyright file="DummyTransferTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class DummyTransferTest : DMLibTestBase
#if DNXCORE50
        , Xunit.IClassFixture<DummyTransferTestFixture>, IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public DummyTransferTest()
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
            Test.Info("Class Initialize: DummyVirtualFolderTest");
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
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.Local)]
        public void DummyDirectoryBlobDownload()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            metadata.Add(Constants.DirectoryBlobMetadataKey, "true");

            Test.Info("Metadata is =====================");
            foreach (var keyValue in metadata)
            {
                Test.Info("name:{0}  value:{1}", keyValue.Key, keyValue.Value);
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode dummyFolderNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = 0L,
                Metadata = metadata
            };

            DirNode dirNode = new DirNode(DMLibTestBase.FileName);
            FileNode actualFile = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
            };
            dirNode.AddFileNode(actualFile);

            sourceDataInfo.RootNode.AddFileNode(dummyFolderNode);
            sourceDataInfo.RootNode.AddDirNode(dirNode);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DestTransferDataInfo = destDataInfo;
            options.DisableDestinationFetch = true;
            options.IsDirectoryTransfer = true;
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");

            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibDataType.Local);
            destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);
            sourceDataInfo.RootNode.DeleteFileNode(DMLibTestBase.FileName);

            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.Local)]
        public void DummySingleBlobDownload()
        {
            // Single dummy blob should behave the same as empty blob.
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            metadata.Add(Constants.DirectoryBlobMetadataKey, "true");

            Test.Info("Metadata is =====================");
            foreach (var keyValue in metadata)
            {
                Test.Info("name:{0}  value:{1}", keyValue.Key, keyValue.Value);
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode dummyFolderNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = 0L,
                Metadata = metadata
            };

            sourceDataInfo.RootNode.AddFileNode(dummyFolderNode);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = false;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");

            dummyFolderNode.Metadata = new Dictionary<string, string>();
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }
    }
}
