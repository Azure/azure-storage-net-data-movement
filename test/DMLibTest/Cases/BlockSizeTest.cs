//------------------------------------------------------------------------------
// <copyright file="BigFileTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class BlockSizeTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public BlockSizeTest()
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
            Test.Info("Class Initialize: BigFileTest");
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
        [DMLibTestMethod(DMLibDataType.Local | DMLibDataType.BlockBlob | DMLibDataType.CloudFile | DMLibDataType.Stream, DMLibDataType.BlockBlob)]
        public void ToLbb_8MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 36*1024); // 36MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 8 * 1024 * 1024 // 8MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.Local | DMLibDataType.BlockBlob | DMLibDataType.CloudFile | DMLibDataType.Stream, DMLibDataType.BlockBlob)]
        public void ToLbb_16MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + 36 + "MB", 36 * 1024); // 36MB, should use 16MB block size, PutBlock and PutBlockList
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + 16 + "MB", 16 * 1024); // 16MB, should use 16MB block size, PutBlob
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + 14 + "MB", 14 * 1024); // 14MB, should use 16MB block size, PutBlob
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + 8 + "MB", 8 * 1024); // 8MB, should use 8MB block size, PutBlob

            for (int i = 0; i < 2; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i); // 0 and 1KB file
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 16 * 1024 * 1024 // 16MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.Local | DMLibDataType.BlockBlob | DMLibDataType.CloudFile | DMLibDataType.Stream, DMLibDataType.BlockBlob)]
        public void ToLbb_100MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 300 * 1024); // 300MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 100 * 1024 * 1024 // 100MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.BlockBlob)]
        public void LbbToLbb_From100MBBlockSizeTo100MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 300 * 1024, blockSize: 100 * 1024 * 1024); // 300MB with block size 100MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 100 * 1024 * 1024 // 100MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.BlockBlob)]
        public void LbbToLbb_From100MBBlockSizeTo8MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 300 * 1024, blockSize: 100 * 1024 * 1024); // 300MB with block size 100MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 8 * 1024 * 1024 // 100MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.Local | DMLibDataType.BlockBlob | DMLibDataType.CloudFile | DMLibDataType.Stream, DMLibDataType.BlockBlob)]
        public void ToLbb_MultiFiles_40MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 36 * 1024); // 36MB
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_150MB", 150 * 1024); // 150MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 40 * 1024 * 1024 // 100MB
            };
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");

            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.BlockBlob)]
        public void LbbToLbb_MultiFiles_60MBBlockSizeAndSmallFiles()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, 36 * 1024); // 36MB
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_150MB", 150 * 1024, blockSize: 100 * 1024 * 1024); // 150MB with block size 100MB

            for (int i = 0; i < 10; i++)
            {
                DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName + "_" + i, i);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                BlockSize = 60*1024*1024 // 100MB
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");

            this.ValidateDestinationMD5ByDownloading(result.DataInfo, options);
        }
    }
}
