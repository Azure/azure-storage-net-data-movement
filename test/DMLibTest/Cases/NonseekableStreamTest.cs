
namespace DMLibTest.Cases
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DMLibTest.Util;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;
    using MS.Test.Common.MsTestLib;
#if DNXCORE50
    using Xunit;

    [Collection(Collections.Global)]
    public class NonseekableStreamTest : DMLibTestBase, IClassFixture<NonseekableStreamTestFixture>, IDisposable
    {
        public NonseekableStreamTest()
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
    [TestClass]
    public class NonseekableStreamTest : DMLibTestBase
    {
#endif
        private static readonly long[] VariedFileSize = new long[] { 0, 1, 4194304};
#region Additional test attributes

        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: NonseekableStreamTest");
            DMLibTestBase.BaseClassInitialize(testContext);
            DMLibTestBase.CleanupSource = false;            
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

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadVariedSizeObject_Nonseekable_NonfixedSized_Positive()
        {
            DMLibDataType[] destTypes = new DMLibDataType[] { DMLibDataType.BlockBlob, DMLibDataType.AppendBlob };
            foreach (var destType in destTypes)
            {
                UploadFromStreamTest(destType, VariedFileSize, null, false, false);
            }
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadLargeObject_Nonseekable_NonfixedSized_Positive()
        {
            DMLibDataType[] destTypes = new DMLibDataType[] { DMLibDataType.BlockBlob, DMLibDataType.AppendBlob };
            foreach (var destType in destTypes)
            {
                int length4MB = 4 * 1024 * 1024;
                int blockSize = (random.Next(length4MB, 100 * 1024 * 1024) / length4MB) * length4MB;

                UploadFromStreamTest(destType, 
                    random.Next(10, 200) * 1024 * 1024,
                    blockSize, 
                    false, 
                    false);
            }
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadVariedSizeObject_Nonseekable_Positive()
        {
            DMLibDataType[] destTypes = new DMLibDataType[] 
            {
                DMLibDataType.BlockBlob,
                DMLibDataType.AppendBlob,
                DMLibDataType.PageBlob,
                DMLibDataType.CloudFile
            };

            foreach (var destType in destTypes)
            {
                if (DMLibDataType.PageBlob != destType)
                {
                    UploadFromStreamTest(destType, VariedFileSize, null, false, true);
                }
                else
                {
                    List<long> variedFileSize = new List<long>();
                    foreach (var fileSize in VariedFileSize)
                    {
                        variedFileSize.Add((int)Math.Ceiling(((double)fileSize) / 512) * 512);
                    }

                    UploadFromStreamTest(destType, variedFileSize.ToArray(), null, false, true);
                }
            }
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadLargeObject_Nonseekable_Positive()
        {
            DMLibDataType[] destTypes = new DMLibDataType[]
            {
                DMLibDataType.BlockBlob,
                DMLibDataType.AppendBlob,
                DMLibDataType.PageBlob,
                DMLibDataType.CloudFile
            };

            foreach (var destType in destTypes)
            {
                int length4MB = 4 * 1024 * 1024;
                int blockSize = (random.Next(length4MB, 100 * 1024 * 1024) / length4MB) * length4MB;

                UploadFromStreamTest(destType,
                    random.Next(10, 200) * 1024 * 1024,
                    blockSize, 
                    false, 
                    true);
            }
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadLargeObject_Nonseekable_NonfixedSize_Nagitive()
        {
            DMLibDataType[] destTypes = new DMLibDataType[]
            {
                DMLibDataType.PageBlob,
                DMLibDataType.CloudFile
            };

            foreach (var destType in destTypes)
            {
                int length4MB = 4 * 1024 * 1024;
                int blockSize = (random.Next(length4MB, 100 * 1024 * 1024) / length4MB) * length4MB;

                UploadFromStreamTest(destType,
                    random.Next(10, 200) * 1024 * 1024,
                    blockSize,
                    false,
                    false);
            }
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void UploadLargeObject_Nonseekable_PageBlob_Nagitive()
        {
            DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(DMLibDataType.Stream);
            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibDataType.PageBlob);

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            sourceDataInfo.RootNode.AddFileNode(new FileNode($"{FileName}_{1}B")
            {
                SizeInByte = 1,
            });

            sourceAdaptor.GenerateData(sourceDataInfo);
            destAdaptor.CreateIfNotExists();

            List<TransferItem> uploadItems = new List<TransferItem>();

            string fileName = $"{FileName}_{1}B";
            FileNode fileNode = sourceDataInfo.RootNode.GetFileNode(fileName);

            uploadItems.Add(new TransferItem()
            {
                SourceObject = new DMLibTestStream(sourceAdaptor.GetTransferObject(string.Empty, fileNode) as FileStream, false, true),
                DestObject = destAdaptor.GetTransferObject(string.Empty, fileNode),
                SourceType = DMLibDataType.Stream,
                DestType = DMLibDataType.PageBlob,
                IsServiceCopy = false
            });

            // Execution
            var result = this.RunTransferItems(
                uploadItems,
                new TestExecutionOptions<DMLibDataInfo>()
                {
                    DisableDestinationFetch = true
                });

            Test.Assert(result.Exceptions.Count == 1 && result.Exceptions[0].Message.Contains("must be a multiple of 512 bytes."), "Verify error is expected.");

            sourceAdaptor.Cleanup();
            destAdaptor.Cleanup();
        }

        private void UploadFromStreamTest(DMLibDataType destType, long sourceLength, int? blockSize, bool seekable, bool fixedSized)
        {
            this.UploadFromStreamTest(destType, new long[] { sourceLength }, blockSize, seekable, fixedSized);
        }

        private void UploadFromStreamTest(DMLibDataType destType, long[] variedSourceLength, int? blockSize, bool seekable, bool fixedSized)
        {
            DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(DMLibDataType.Stream);
            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(destType);

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            foreach (long fileSizeInByte in variedSourceLength)
            {
                sourceDataInfo.RootNode.AddFileNode(new FileNode($"{FileName}_{fileSizeInByte}")
                {
                    SizeInByte = fileSizeInByte,
                });
            }

            sourceAdaptor.GenerateData(sourceDataInfo);
            destAdaptor.CreateIfNotExists();

            List<TransferItem> uploadItems = new List<TransferItem>();

            foreach (long fileSizeInByte in variedSourceLength)
            {
                string fileName = $"{FileName}_{fileSizeInByte}";
                FileNode fileNode = sourceDataInfo.RootNode.GetFileNode(fileName);

                uploadItems.Add(new TransferItem()
                {
                    SourceObject = new DMLibTestStream(sourceAdaptor.GetTransferObject(string.Empty, fileNode) as FileStream, seekable, fixedSized),
                    DestObject = destAdaptor.GetTransferObject(string.Empty, fileNode),
                    SourceType = DMLibDataType.Stream,
                    DestType = destType,
                    IsServiceCopy = false
                });
            }

            // Execution
            var result = this.RunTransferItems(
                uploadItems, 
                new TestExecutionOptions<DMLibDataInfo>()
                {
                    DisableDestinationFetch = true,
                    BlockSize = blockSize.HasValue ? blockSize.Value : 4 * 1024 * 1024
                });

            if (!fixedSized && (destType == DMLibDataType.PageBlob || destType == DMLibDataType.CloudFile))
            {
                Test.Assert(result.Exceptions.Count == 1 && result.Exceptions[0].Message.Contains("Source must be fixed size"), "Verify error is expected.");

            }
            else
            {
                // Verify all files are transfered successfully
                Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");

                DMLibDataInfo destDataInfo = destAdaptor.GetTransferDataInfo(string.Empty);

                foreach (FileNode destFileNode in destDataInfo.EnumerateFileNodes())
                {
                    FileNode sourceFileNode = sourceDataInfo.RootNode.GetFileNode(destFileNode.Name);
                    Test.Assert(DMLibDataHelper.Equals(sourceFileNode, destFileNode), "Verify transfer result.");
                }
            }

            sourceAdaptor.Cleanup();
            destAdaptor.Cleanup();
        }
    }
}
