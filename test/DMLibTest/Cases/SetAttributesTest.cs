namespace DMLibTest.Cases
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
    public class SetAttributesTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public SetAttributesTest()
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
            Test.Info("Class Initialize: SetAttributeTest");
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
            this.BaseTestInitialize();
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            this.BaseTestCleanup();
        }

        #endregion

#if DNXCORE50

        // It doesn't work in .Net Core to get content type value without subtype.
        // Content type should always include a subtype, here change test cases according to it.
        // Should also test the feature to set content type without subtype when .Net Core fixes its issue.
        private const string TestContentType = "newtype/subtype";
#else
        private const string TestContentType = "newtype";
#endif
        private const string invalidMD5 = "ThisIsAnInvalidMD5MD5A==";
        
        private static readonly string StraightALongString = new string('a', 1024);

        private static readonly string RandomLongString = FileOp.NextNormalString(random, 1024);

        private static readonly string MetadataKey1 = FileOp.NextCIdentifierString(random);

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        public void TestDirectorySetAttributesToLocal()
        {
            TestSetAttributesToLocal(true);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.Local)]
        public void TestSetAttributesToLocal()
        {
            TestSetAttributesToLocal(false);
        }

        private void TestSetAttributesToLocal(bool IsDirectoryTransfer)
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB* 1024L, 
            };

            sourceDataInfo.RootNode.AddFileNode(fileNode);

            TransferContext context;
            
            if (IsDirectoryTransfer)
            {
                context = new DirectoryTransferContext()
                {
                    SetAttributesCallbackAsync = async (destObj) =>
                    {
                        Test.Error("SetAttributes callback should not be invoked when destination is local");
                    }
                };
            }
            else
            {
                context = new SingleTransferContext()
                {
                    SetAttributesCallbackAsync = async (destObj) =>
                    {
                        Test.Error("SetAttributes callback should not be invoked when destination is local");
                    }
                };
            }

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = IsDirectoryTransfer ? DefaultTransferDirectoryOptions : DefaultTransferOptions;

                if (IsDirectoryTransfer)
                {
                    transferOptions.Recursive = true;
                }

                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };

            options.IsDirectoryTransfer = IsDirectoryTransfer;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudDest)]
        public void TestSetAttributes()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            if (DMLibTestContext.SourceType != DMLibDataType.Local)
            {
                metadata.Add("foo", "bar");
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName)
            {
                SizeInByte = DMLibTestBase.FileSizeInKB * 1024L, 
                Metadata = metadata
            };

            sourceDataInfo.RootNode.AddFileNode(fileNode);

            TransferContext context = new SingleTransferContext()
            {
                SetAttributesCallbackAsync = async (destObj) =>
                {
                    dynamic destCloudObj = destObj;

                    destCloudObj.Properties.ContentType = SetAttributesTest.TestContentType;

                    destCloudObj.Metadata.Add("aa", "bb");
                }
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;
                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            fileNode.Metadata.Add("aa", "bb");

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

            FileNode destFileNode = result.DataInfo.RootNode.GetFileNode(DMLibTestBase.FileName);
            Test.Assert(TestContentType.Equals(destFileNode.ContentType), "Verify content type: {0}, expected {1}", destFileNode.ContentType, TestContentType);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudDest)]
        public void TestDirectorySetAttributes()
        {
            Dictionary<string, string> metadata = new Dictionary<string, string>();
            if (DMLibTestContext.SourceType != DMLibDataType.Local)
            {
                metadata.Add("foo", "bar");
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            for (int i = 0; i < 3; ++i)
            {
                FileNode fileNode = new FileNode(DMLibTestBase.FileName + i)
                {
                    SizeInByte = DMLibTestBase.FileSizeInKB * 1024L, 
                    Metadata = metadata
                };

                sourceDataInfo.RootNode.AddFileNode(fileNode);
            }

            DirectoryTransferContext context = new DirectoryTransferContext()
            {
                SetAttributesCallbackAsync = async (destObj) =>
                {
                    dynamic destCloudObj = destObj;

                    destCloudObj.Properties.ContentType = SetAttributesTest.TestContentType;

                    destCloudObj.Metadata.Add("aa", "bb");
                }
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (node, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
                transferItem.TransferContext = context;
            };
            options.IsDirectoryTransfer = true;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            foreach (FileNode fileNode in sourceDataInfo.EnumerateFileNodes())
            {
                fileNode.Metadata.Add("aa", "bb");
            }

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
            VerificationHelper.VerifySingleTransferStatus(result, 3, 0, 0, 3 * DMLibTestBase.FileSizeInKB * 1024L);

            foreach(FileNode destFileNode in result.DataInfo.EnumerateFileNodes())
            {
                Test.Assert(TestContentType.Equals(destFileNode.ContentType), "Verify content type: {0}, expected {1}", destFileNode.ContentType, TestContentType);
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudBlob)]
        public void TestDirectorySetAttribute_Restart_Copy()
        {
            int bigFileSizeInKB = 5 * 1024; // 5 MB
            int smallFileSizeInKB = 1; // 1 KB
            int bigFileNum = 20;
            int smallFileNum = 50;

            string longString = StraightALongString;

#if BINARY_SERIALIZATION
            longString = RandomLongString;
#endif // For .Net core, blob readers cannot fetch contentType and etc with special chars.

            Dictionary<string, string> metadata = new Dictionary<string, string> { { MetadataKey1, longString } };
            
            this.TestDirectorySetAttribute_Restart(
                bigFileSizeInKB, 
                smallFileSizeInKB, 
                bigFileNum, 
                smallFileNum, 
                bigFileDirNode =>
                    {
                        DMLibDataHelper.AddMultipleFiles(
                            bigFileDirNode, 
                            FileName, 
                            bigFileNum, 
                            bigFileSizeInKB, 
                            cacheControl: longString, 
                            contentDisposition: longString, 
                            contentEncoding: longString, 
                            contentLanguage: longString, 
                            contentType: longString, 
                            md5: invalidMD5, 
                            metadata: metadata);
                    }, 
                smallFileDirNode =>
                    {
                        DMLibDataHelper.AddMultipleFiles(
                        smallFileDirNode, 
                        FileName, 
                        smallFileNum, 
                        smallFileSizeInKB, 
                        cacheControl: longString, 
                        contentDisposition: longString, 
                        contentEncoding: longString, 
                        contentLanguage: longString, 
                        contentType: longString, 
                        md5: invalidMD5, 
                        metadata: metadata);
                    }
            );
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.BlockBlob)]
        public void TestDirectorySetAttribute_Restart_Upload()
        {
            int bigFileSizeInKB = 5 * 1024; // 5 MB
            int smallFileSizeInKB = 1; // 1 KB
            int bigFileNum = 20;
            int smallFileNum = 50;

            this.TestDirectorySetAttribute_Restart(
                bigFileSizeInKB, 
                smallFileSizeInKB, 
                bigFileNum, 
                smallFileNum, 
                bigFileDirNode =>
                {
                    DMLibDataHelper.AddMultipleFiles(
                        bigFileDirNode, 
                        FileName, 
                        bigFileNum, 
                        bigFileSizeInKB);
                }, 
                smallFileDirNode =>
                {
                    DMLibDataHelper.AddMultipleFiles(
                    smallFileDirNode, 
                    FileName, 
                    smallFileNum, 
                    smallFileSizeInKB);
                }, 
                async (destObj) =>
                {
                    dynamic destCloudObj = destObj;

                    destCloudObj.Properties.ContentType = RandomLongString;
                    destCloudObj.Properties.CacheControl = RandomLongString;
                    destCloudObj.Properties.ContentDisposition = RandomLongString;
                    destCloudObj.Properties.ContentEncoding = RandomLongString;
                    destCloudObj.Properties.ContentLanguage = RandomLongString;
                    destCloudObj.Properties.ContentType = RandomLongString;
                    destCloudObj.Properties.ContentMD5 = invalidMD5;

                    destCloudObj.Metadata.Remove(MetadataKey1);
                    destCloudObj.Metadata.Add(MetadataKey1, RandomLongString);
                },
                sourceDataInfo =>
                {
                    foreach (var fileNode in sourceDataInfo.EnumerateFileNodes())
                    {
                        fileNode.ContentType = RandomLongString;
                        fileNode.CacheControl = RandomLongString;
                        fileNode.ContentDisposition = RandomLongString;
                        fileNode.ContentEncoding = RandomLongString;
                        fileNode.ContentLanguage = RandomLongString;
                        fileNode.ContentType = RandomLongString;
                        fileNode.MD5 = invalidMD5;

                        fileNode.Metadata = new Dictionary<string, string> { { MetadataKey1, RandomLongString } };
                    }
                }
            );
        }

        private void TestDirectorySetAttribute_Restart(
            int bigFileSizeInKB, 
            int smallFileSizeInKB, 
            int bigFileNum, 
            int smallFileNum, 
            Action<DirNode> bigFileDirAddFileAction, 
            Action<DirNode> smallFileDirAddFileAction, 
            SetAttributesCallbackAsync setAttributesCallback = null, 
            Action<DMLibDataInfo> sourceDataInfoDecorator = null)
        {
            int totalFileNum = bigFileNum + smallFileNum;
            long totalSizeInBytes = ((bigFileSizeInKB * bigFileNum) + (smallFileSizeInKB * smallFileNum)) * 1024;

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DirNode bigFileDirNode = new DirNode("big");
            DirNode smallFileDirNode = new DirNode("small");

            sourceDataInfo.RootNode.AddDirNode(bigFileDirNode);
            sourceDataInfo.RootNode.AddDirNode(smallFileDirNode);
            bigFileDirAddFileAction(bigFileDirNode);
            smallFileDirAddFileAction(smallFileDirNode);

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo> { LimitSpeed = true, IsDirectoryTransfer = true };

            using (Stream journalStream = new MemoryStream())
            {
                bool isStreamJournal = random.Next(0, 2) == 0;

                var transferContext = isStreamJournal ? new DirectoryTransferContext(journalStream) : new DirectoryTransferContext();
                transferContext.SetAttributesCallbackAsync = setAttributesCallback;

                var progressChecker = new ProgressChecker(totalFileNum, totalSizeInBytes, totalFileNum, null, 0, totalSizeInBytes);
                transferContext.ProgressHandler = progressChecker.GetProgressHandler();
                
                var eventChecker = new TransferEventChecker();
                eventChecker.Apply(transferContext);

                transferContext.FileFailed += (sender, e) =>
                {
                    Helper.VerifyCancelException(e.Exception);
                };

                options.TransferItemModifier = (fileName, item) =>
                {
                    dynamic dirOptions = DefaultTransferDirectoryOptions;
                    dirOptions.Recursive = true;

                    item.Options = dirOptions;
                    item.CancellationToken = tokenSource.Token;
                    item.TransferContext = transferContext;
                    transferItem = item;
                };

                TransferCheckpoint firstCheckpoint = null, secondCheckpoint = null;
                options.AfterAllItemAdded = () =>
                {
                    // Wait until there are data transferred
                    progressChecker.DataTransferred.WaitOne();

                    if (!isStreamJournal)
                    {
                        // Store the first checkpoint
                        firstCheckpoint = transferContext.LastCheckpoint;
                    }

                    Thread.Sleep(1000);

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                };

                // Cancel and store checkpoint for resume
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                if (progressChecker.FailedFilesNumber <= 0)
                {
                    Test.Error("Verify file number in progress. Failed: {0}", progressChecker.FailedFilesNumber);
                }

                TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;

                if (!isStreamJournal)
                {
                    secondCheckpoint = transferContext.LastCheckpoint;

                    Test.Info("Resume with the second checkpoint first.");
                    firstResumeCheckpoint = secondCheckpoint;
                    secondResumeCheckpoint = firstCheckpoint;
                }

                // resume with firstResumeCheckpoint
                TransferItem resumeItem = transferItem.Clone();

                progressChecker.Reset();
                TransferContext resumeContext = null;

                if (isStreamJournal)
                {
                    resumeContext = new DirectoryTransferContext(journalStream)
                    {
                        ProgressHandler = progressChecker.GetProgressHandler()
                    };
                }
                else
                {
                    resumeContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(firstResumeCheckpoint))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler()
                    };
                }
                
                resumeContext.SetAttributesCallbackAsync = setAttributesCallback;

                eventChecker.Reset();
                eventChecker.Apply(resumeContext);

                resumeItem.TransferContext = resumeContext;

                result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                sourceDataInfoDecorator?.Invoke(sourceDataInfo);

                VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
                VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);
                VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                if (!isStreamJournal)
                {
                    // resume with secondResumeCheckpoint
                    resumeItem = transferItem.Clone();

                    progressChecker.Reset();
                    resumeContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler(), 

                        // Need this overwrite callback since some files is already transferred to destination
                        ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY(), 

                        SetAttributesCallbackAsync = setAttributesCallback
                    };

                    eventChecker.Reset();
                    eventChecker.Apply(resumeContext);

                    resumeItem.TransferContext = resumeContext;

                    result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                    VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
                    VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);
                    VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
                }
            }
        }
    }
}
