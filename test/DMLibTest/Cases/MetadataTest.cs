//------------------------------------------------------------------------------
// <copyright file="MetadataTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System.Collections.Generic;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;
    using System;
    using Microsoft.Azure.Storage.DataMovement;

    [MultiDirectionTestClass]
    public class MetadataTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public MetadataTest()
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
            Test.Info("Class Initialize: MetadataTest");
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
        [DMLibTestMethodSet(DMLibTestMethodSet.Cloud2Cloud)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void TestMetadata()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            metadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));
            metadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));

            Test.Info("Metadata is =====================");
            foreach (var keyValue in metadata)
            {
                Test.Info("name:{0}  value:{1}", keyValue.Key, keyValue.Value);
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
                Metadata = metadata
            };
            sourceDataInfo.RootNode.AddFileNode(fileNode);

            var result = this.ExecuteTestCase(sourceDataInfo, new TestExecutionOptions<DMLibDataInfo>());

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.BlockBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.BlockBlob, DMLibCopyMethod.ServiceSideAsyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void TestMetadataOverwrite()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            metadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));
            metadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));

            Test.Info("Metadata is =====================");
            foreach (var keyValue in metadata)
            {
                Test.Info("name:{0}  value:{1}", keyValue.Key, keyValue.Value);
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
                Metadata = metadata
            };
            sourceDataInfo.RootNode.AddFileNode(fileNode);

            Dictionary<string, string> destMetadata = new Dictionary<string, string>();
            destMetadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));
            destMetadata.Add(FileOp.NextCIdentifierString(random), FileOp.NextNormalString(random));

            Test.Info("Destination metadata is =====================");
            foreach (var keyValue in destMetadata)
            {
                Test.Info("name:{0}  value:{1}", keyValue.Key, keyValue.Value);
            }

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L,
                Metadata = destMetadata
            };
            destDataInfo.RootNode.AddFileNode(fileNode);
            var option = new TestExecutionOptions<DMLibDataInfo>()
                {
                    DestTransferDataInfo = destDataInfo
                };

            SingleTransferContext transferContext = new SingleTransferContext();
            transferContext.ShouldOverwriteCallbackAsync = async (source, dest) =>
            {
                return true;
            };

            option.TransferItemModifier = (inputFileNode, transferItem) =>
            {
                transferItem.TransferContext = transferContext;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, option);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }
    }
}
