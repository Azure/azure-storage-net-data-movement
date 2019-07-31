//------------------------------------------------------------------------------
// <copyright file="BVT.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class BVT : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public BVT()
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
            Test.Info("Class Initialize: BVT");
            DMLibTestBase.BaseClassInitialize(testContext);
            BVT.UnicodeFileName = FileOp.NextString(random, random.Next(6, 10));
            Test.Info("Use file name {0} in BVT.", UnicodeFileName);
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

        private static string UnicodeFileName;

        [TestCategory(Tag.BVT)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void TransferDirectoryDifferentSizeObject()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            DMLibDataHelper.AddMultipleFilesNormalSize(sourceDataInfo.RootNode, BVT.UnicodeFileName);

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    item.Options = transferOptions;
                },
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // For sync copy and service side sync copy, recalculate md5 of destination by downloading the file to local.
            if (IsCloudService(DMLibTestContext.DestType) && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
            {
                DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
            }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.BVT)]
        [DMLibTestMethodSet(DMLibTestMethodSet.AllValidDirection)]
        public void TransferDifferentSizeObject()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddMultipleFilesNormalSize(sourceDataInfo.RootNode, BVT.UnicodeFileName);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.AfterDataPrepared = () =>
                {
                    if ((DMLibTestContext.SourceType == DMLibDataType.CloudFile || DMLibTestContext.SourceType == DMLibDataType.PageBlob) &&
                        (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                    {
                        string sparseFileName = "SparseFile";

                        DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, sparseFileName, 1);
                        FileNode sparseFileNode = sourceDataInfo.RootNode.GetFileNode(sparseFileName);

                        if (DMLibTestContext.SourceType == DMLibDataType.CloudFile)
                        {
                            CloudFileDataAdaptor cloudFileDataAdaptor = SourceAdaptor as CloudFileDataAdaptor;
                            CloudFile sparseCloudFile = cloudFileDataAdaptor.GetCloudFileReference(sourceDataInfo.RootPath, sparseFileNode);
                            this.PrepareCloudFileWithDifferentSizeRange(sparseCloudFile);
                            sparseFileNode.MD5 = sparseCloudFile.Properties.ContentMD5;
                            sparseFileNode.Metadata = sparseCloudFile.Metadata;
                        }
                        else if (DMLibTestContext.SourceType == DMLibDataType.PageBlob)
                        {
                            CloudBlobDataAdaptor cloudBlobDataAdaptor = SourceAdaptor as CloudBlobDataAdaptor;
                            CloudPageBlob sparsePageBlob = cloudBlobDataAdaptor.GetCloudBlobReference(sourceDataInfo.RootPath, sparseFileNode) as CloudPageBlob;
                            this.PreparePageBlobWithDifferenSizePage(sparsePageBlob);
                            sparseFileNode.MD5 = sparsePageBlob.Properties.ContentMD5;
                            sparseFileNode.Metadata = sparsePageBlob.Metadata;
                        }
                    }
                };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

             // For sync copy, recalculate md5 of destination by downloading the file to local.
             if (IsCloudService(DMLibTestContext.DestType) && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
             {
                 DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
             }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        private void PreparePageBlobWithDifferenSizePage(CloudPageBlob pageBlob)
        {
            List<int> ranges = new List<int>();
            List<int> gaps = new List<int>();

            // Add one 4MB - 16MB page, align with 512 byte
            ranges.Add(random.Next(4 * 2 * 1024, 16 * 2 * 1024) * 512);

            // Add one 512B page
            ranges.Add(512);

            int remainingPageNumber = random.Next(10, 20);

            // Add ten - twenty 512B - 4MB page, align with 512 byte
            for (int i = 0; i < remainingPageNumber; ++i)
            {
                ranges.Add(random.Next(1, 4 * 2 * 1024) * 512);
            }

            // Add one 4M - 8M gap, align with 512 byte
            gaps.Add(random.Next(4 * 2 * 1024, 8 * 2 * 1024) * 512);

            // Add 512B - 2048B gaps, align with 512 byte
            for (int i = 1; i < ranges.Count - 1; ++i)
            {
                gaps.Add(random.Next(1, 5) * 512);
            }

            ranges.Shuffle();
            gaps.Shuffle();

            CloudBlobHelper.GeneratePageBlobWithRangedData(pageBlob, ranges, gaps);
        }

        private void PrepareCloudFileWithDifferentSizeRange(CloudFile cloudFile)
        {
            List<int> ranges = new List<int>();
            List<int> gaps = new List<int>();

            // Add one 4MB - 16MB range
            ranges.Add(random.Next(4 * 1024 * 1024, 16 * 1024 * 1024));

            // Add one 1B range
            ranges.Add(1);

            int remainingPageNumber = random.Next(10, 20);

            // Add ten - twenty 1B - 4MB range
            for (int i = 0; i < remainingPageNumber; ++i)
            {
                ranges.Add(random.Next(1, 4 * 1024 * 1024));
            }

            // Add one 4M - 8M gap
            gaps.Add(random.Next(4 * 1024 * 1024, 8 * 1024 * 1024));

            // Add 512B - 2048B gaps
            for (int i = 1; i < ranges.Count - 1; ++i)
            {
                gaps.Add(random.Next(1, 512 * 4));
            }

            if (DMLibTestContext.DestType == DMLibDataType.PageBlob)
            {
                int totalSize = ranges.Sum() + gaps.Sum();
                int remainder = totalSize % 512;

                if (remainder != 0)
                {
                    ranges[ranges.Count - 1] += 512 - remainder;
                }
            }

            ranges.Shuffle();
            gaps.Shuffle();

            CloudFileHelper.GenerateCloudFileWithRangedData(cloudFile, ranges, gaps);
        }
    }
}
