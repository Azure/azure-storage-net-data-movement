//------------------------------------------------------------------------------
// <copyright file="LongFilePathTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Cases
{

    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Threading;

    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using DMLibTest.Framework;
    using System.Threading.Tasks;
#if DNXCORE50
    using Xunit;
    using System.Threading.Tasks;

    [MultiDirectionTestClass]
    public class LongFilePathTest : DMLibTestBase, IClassFixture<LongFilePathTestFixture>, IDisposable
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
        private int pathLengthLimit = 32 * 1000;
        private string sourceDirectoryName = string.Empty;
        private string destDirectoryName = string.Empty;

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
#if DOTNET5_4
            if (!CrossPlatformHelpers.IsWindows)
            {
                // Ubuntu has a maximum path of 4096 characters.
                pathLengthLimit = 4095;
            }
#endif
            sourceDirectoryName = LongPathExtension.Combine(Directory.GetCurrentDirectory(), SourceRoot+ DMLibTestHelper.RandomNameSuffix());
            destDirectoryName = LongPathExtension.Combine(Directory.GetCurrentDirectory(), DestRoot+ DMLibTestHelper.RandomNameSuffix());
            base.BaseTestInitialize();
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            if (LongPathDirectoryExtension.Exists(sourceDirectoryName))
                Helper.CleanupFolder(sourceDirectoryName);
            if (LongPathDirectoryExtension.Exists(destDirectoryName))
                Helper.CleanupFolder(destDirectoryName);
            base.BaseTestCleanup();
        }
        #endregion

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathSingleUpload()
        {
            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(sourceDirectoryName, DMLibTestBase.FileName, pathLengthLimit));
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            var options = new TestExecutionOptions<DMLibDataInfo>();

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.Local, DMLibDataType.CloudFile)]
        public void LongFilePathSingleUploadPreserveSMBAttributes()
        {
            if (!CrossPlatformHelpers.IsWindows)
            {
                return;
            }

            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(sourceDirectoryName, DMLibTestBase.FileName, pathLengthLimit));
            FileNode fileNode = new FileNode(DMLibTestBase.FileName);
            fileNode.SizeInByte = fileSizeInKB;
            fileNode.SMBAttributes = CloudFileNtfsAttributes.Hidden;
            sourceDataInfo.RootNode.AddFileNode(fileNode);

            LocalDataAdaptor sourceAdaptor = GetDestAdaptor(DMLibDataType.Local) as LocalDataAdaptor;
            sourceAdaptor.GenerateDataInfo(sourceDataInfo, true);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DisableSourceGenerator = true;
            options.TransferItemModifier = (fileNodeVar, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;
                transferOptions.PreserveSMBAttributes = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            Helper.CompareSMBProperties(sourceDataInfo.RootNode, result.DataInfo.RootNode, true);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.Local)]
        public void LongFilePathSingleDownloadPreserveSMBAttributes()
        {
            if (!CrossPlatformHelpers.IsWindows)
            {
                return;
            }

            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            FileNode fileNode = new FileNode(DMLibTestBase.FileName);
            fileNode.SizeInByte = fileSizeInKB;
            fileNode.SMBAttributes = CloudFileNtfsAttributes.Hidden;
            sourceDataInfo.RootNode.AddFileNode(fileNode);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(destDirectoryName, DMLibTestBase.FileName, pathLengthLimit));
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DestTransferDataInfo = destDataInfo;
            options.DisableDestinationFetch = true;

            options.TransferItemModifier = (fileNodeVar, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;
                transferOptions.PreserveSMBAttributes = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            LocalDataAdaptor destAdaptor = GetDestAdaptor(DMLibDataType.Local) as LocalDataAdaptor;
            destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath, true);
            
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");

            Helper.CompareSMBProperties(sourceDataInfo.RootNode, destDataInfo.RootNode, true);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongFilePathSingleDownload()
        {
            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);
            
            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(destDirectoryName, DMLibTestBase.FileName, pathLengthLimit));

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DestTransferDataInfo = destDataInfo;
            options.DisableDestinationFetch = true;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibDataType.Local);
            destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathResumeSingleUpload()
        {
            int fileSizeInKB = 100 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(sourceDirectoryName, DMLibTestBase.FileName, pathLengthLimit));
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.LimitSpeed = true;

            bool IsStreamJournal = random.Next(0, 2) == 0;
            using (Stream journalStream = new MemoryStream())
            {
                TransferContext transferContext = IsStreamJournal ? new SingleTransferContext(journalStream) : new SingleTransferContext();
                var progressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 0, 1, 0, fileSizeInKB * 1024);
                transferContext.ProgressHandler = progressChecker.GetProgressHandler();
                options.TransferItemModifier = (fileName, item) =>
                {
                    item.CancellationToken = tokenSource.Token;
                    item.TransferContext = transferContext;
                    transferItem = item;
                };

                TransferCheckpoint firstCheckpoint = null, secondCheckpoint = null;
                options.AfterAllItemAdded = () =>
                {
                    if (IsStreamJournal &&
                        (DMLibTestContext.SourceType == DMLibDataType.Stream
                        || DMLibTestContext.DestType == DMLibDataType.Stream))
                    {
                        return;
                    }

                    // Wait until there are data transferred
                    progressChecker.DataTransferred.WaitOne(30000);

                    // Store the first checkpoint
                    if (!IsStreamJournal)
                    {
                        firstCheckpoint = transferContext.LastCheckpoint;
                    }
                    Thread.Sleep(1000);

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                };

                // Cancel and store checkpoint for resume
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                if (!IsStreamJournal)
                {
                    secondCheckpoint = transferContext.LastCheckpoint;
                }
                else
                {
                    if (DMLibTestContext.SourceType == DMLibDataType.Stream || DMLibTestContext.DestType == DMLibDataType.Stream)
                    {
                        Test.Assert(result.Exceptions.Count == 1, "Verify job is failed");
                        Exception jobException = result.Exceptions[0];
                        Test.Info("{0}", jobException);
                        VerificationHelper.VerifyExceptionErrorMessage(jobException, "Cannot deserialize to TransferLocation when its TransferLocationType is Stream.");
                        return;
                    }
                }

                Test.Assert(result.Exceptions.Count == 1, "Verify job is cancelled");
                Exception exception = result.Exceptions[0];
                Helper.VerifyCancelException(exception);

                TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;
                ProgressChecker firstProgressChecker = null, secondProgressChecker = null;

                if (!IsStreamJournal)
                {
                    // DMLib doesn't support to resume transfer from a checkpoint which is inconsistent with
                    // the actual transfer progress when the destination is an append blob.
                    if (Helper.RandomBoolean() && (DMLibTestContext.DestType != DMLibDataType.AppendBlob || (DMLibTestContext.CopyMethod == DMLibCopyMethod.ServiceSideAsyncCopy)))
                    {
                        Test.Info("Resume with the first checkpoint first.");
                        firstResumeCheckpoint = firstCheckpoint;
                        secondResumeCheckpoint = secondCheckpoint;
                    }
                    else
                    {
                        Test.Info("Resume with the second checkpoint first.");
                        firstResumeCheckpoint = secondCheckpoint;
                        secondResumeCheckpoint = firstCheckpoint;
                    }
                }

                // first progress checker
                if (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType != DMLibDataType.BlockBlob)
                {
                    // The destination is already created, will cause a transfer skip
                    firstProgressChecker = new ProgressChecker(2, fileSizeInKB * 1024, 0, 1 /* failed */, 1 /* skipped */, fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.Stream || (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType == DMLibDataType.BlockBlob))
                {
                    firstProgressChecker = new ProgressChecker(2, 2 * fileSizeInKB * 1024, 1 /* transferred */, 1 /* failed */, 0, 2 * fileSizeInKB * 1024);
                }
                else
                {
                    firstProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 1, 0, 0, fileSizeInKB * 1024);
                }

                // second progress checker
                if (DMLibTestContext.SourceType == DMLibDataType.Stream)
                {
                    // The destination is already created, will cause a transfer skip
                    secondProgressChecker = new ProgressChecker(2, fileSizeInKB * 1024, 0, 1 /* failed */, 1 /* skipped */, fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.Stream)
                {
                    secondProgressChecker = new ProgressChecker(2, 2 * fileSizeInKB * 1024, 1 /* transferred */, 1 /* failed */, 0, 2 * fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.AppendBlob && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                {
                    secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 0, 1 /* failed */, 0, fileSizeInKB * 1024);
                }
                else
                {
                    secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 1 /* transferred */, 0, 0, fileSizeInKB * 1024);
                }

                // resume with firstResumeCheckpoint
                TransferItem resumeItem = transferItem.Clone();
                TransferContext resumeContext = null;

                if (IsStreamJournal)
                {
                    Exception deserializeEX = null;
                    try
                    {
                        resumeContext = new SingleTransferContext(journalStream)
                        {
                            ProgressHandler = firstProgressChecker.GetProgressHandler()
                        };
                    }
                    catch (Exception ex)
                    {
                        if ((DMLibTestContext.SourceType != DMLibDataType.Stream)
                            && (DMLibTestContext.DestType != DMLibDataType.Stream))
                        {
                            Test.Error("Should no exception in deserialization when no target is stream.");
                        }

                        deserializeEX = ex;
                    }
                }
                else
                {
                    resumeContext = new SingleTransferContext(IsStreamDirection() ? firstResumeCheckpoint : DMLibTestHelper.RandomReloadCheckpoint(firstResumeCheckpoint))
                    {
                        ProgressHandler = firstProgressChecker.GetProgressHandler()
                    };
                }

                resumeItem.TransferContext = resumeContext;

                result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                if (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType != DMLibDataType.BlockBlob)
                {
                    Test.Assert(result.Exceptions.Count == 1, "Verify transfer is skipped when source is stream.");
                    exception = result.Exceptions[0];
                    VerificationHelper.VerifyTransferException(result.Exceptions[0], TransferErrorCode.NotOverwriteExistingDestination, "Skipped file");
                }
                else
                {
                    // For sync copy, recalculate md5 of destination by downloading the file to local.
                    if (IsCloudService(DMLibTestContext.DestType) && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                    {
                        DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
                    }

                    VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);
                }

                if (!IsStreamJournal)
                {
                    // resume with secondResumeCheckpoint
                    resumeItem = transferItem.Clone();
                    resumeContext = new SingleTransferContext(
                        IsStreamDirection() ? secondResumeCheckpoint : DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
                    {
                        ProgressHandler = secondProgressChecker.GetProgressHandler()
                    };
                    resumeItem.TransferContext = resumeContext;

                    result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                    if (DMLibTestContext.SourceType == DMLibDataType.Stream)
                    {
                        Test.Assert(result.Exceptions.Count == 1, "Verify transfer is skipped when source is stream.");
                        exception = result.Exceptions[0];
                        VerificationHelper.VerifyTransferException(result.Exceptions[0], TransferErrorCode.NotOverwriteExistingDestination, "Skipped file");
                    }
                    else if (DMLibTestContext.DestType == DMLibDataType.AppendBlob && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                    {
                        Test.Assert(result.Exceptions.Count == 1, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                        exception = result.Exceptions[0];
                        Test.Assert(exception is InvalidOperationException, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                        VerificationHelper.VerifyExceptionErrorMessage(exception, "Destination might be changed by other process or application.");
                    }
                    else
                    {
                        // For sync copy, recalculate md5 of destination by downloading the file to local.
                        if (IsCloudService(DMLibTestContext.DestType) && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                        {
                            DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
                        }

                        VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);
                    }
                }
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalDest)]
        public void LongFilePathResumeSingleDownload()
        {
            int fileSizeInKB = 100 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(destDirectoryName, DMLibTestBase.FileName, pathLengthLimit));

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DestTransferDataInfo = destDataInfo;
            options.LimitSpeed = true;
            options.DisableDestinationFetch = true;

            bool IsStreamJournal = random.Next(0, 2) == 0;
            using (Stream journalStream = new MemoryStream())
            {
                TransferContext transferContext = IsStreamJournal ? new SingleTransferContext(journalStream) : new SingleTransferContext();
                var progressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 0, 1, 0, fileSizeInKB * 1024);
                transferContext.ProgressHandler = progressChecker.GetProgressHandler();
                options.TransferItemModifier = (fileName, item) =>
                {
                    item.CancellationToken = tokenSource.Token;
                    item.TransferContext = transferContext;
                    transferItem = item;
                };

                TransferCheckpoint firstCheckpoint = null, secondCheckpoint = null;
                options.AfterAllItemAdded = () =>
                {
                    if (IsStreamJournal &&
                        (DMLibTestContext.SourceType == DMLibDataType.Stream
                        || DMLibTestContext.DestType == DMLibDataType.Stream))
                    {
                        return;
                    }

                    // Wait until there are data transferred
                    progressChecker.DataTransferred.WaitOne(30000);

                    // Store the first checkpoint
                    if (!IsStreamJournal)
                    {
                        firstCheckpoint = transferContext.LastCheckpoint;
                    }
                    Thread.Sleep(1000);

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                };

                // Cancel and store checkpoint for resume
                var result = this.ExecuteTestCase(sourceDataInfo, options);

                if (!IsStreamJournal)
                {
                    secondCheckpoint = transferContext.LastCheckpoint;
                }
                else
                {
                    if (DMLibTestContext.SourceType == DMLibDataType.Stream || DMLibTestContext.DestType == DMLibDataType.Stream)
                    {
                        Test.Assert(result.Exceptions.Count == 1, "Verify job is failed");
                        Exception jobException = result.Exceptions[0];
                        Test.Info("{0}", jobException);
                        VerificationHelper.VerifyExceptionErrorMessage(jobException, "Cannot deserialize to TransferLocation when its TransferLocationType is Stream.");
                        return;
                    }
                }

                Test.Assert(result.Exceptions.Count == 1, "Verify job is cancelled");
                Exception exception = result.Exceptions[0];
                Helper.VerifyCancelException(exception);

                TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;
                ProgressChecker firstProgressChecker = null, secondProgressChecker = null;

                if (!IsStreamJournal)
                {
                    // DMLib doesn't support to resume transfer from a checkpoint which is inconsistent with
                    // the actual transfer progress when the destination is an append blob.
                    if (Helper.RandomBoolean() && (DMLibTestContext.DestType != DMLibDataType.AppendBlob || (DMLibTestContext.CopyMethod == DMLibCopyMethod.ServiceSideAsyncCopy)))
                    {
                        Test.Info("Resume with the first checkpoint first.");
                        firstResumeCheckpoint = firstCheckpoint;
                        secondResumeCheckpoint = secondCheckpoint;
                    }
                    else
                    {
                        Test.Info("Resume with the second checkpoint first.");
                        firstResumeCheckpoint = secondCheckpoint;
                        secondResumeCheckpoint = firstCheckpoint;
                    }
                }

                // first progress checker
                if (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType != DMLibDataType.BlockBlob)
                {
                    // The destination is already created, will cause a transfer skip
                    firstProgressChecker = new ProgressChecker(2, fileSizeInKB * 1024, 0, 1 /* failed */, 1 /* skipped */, fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.Stream || (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType == DMLibDataType.BlockBlob))
                {
                    firstProgressChecker = new ProgressChecker(2, 2 * fileSizeInKB * 1024, 1 /* transferred */, 1 /* failed */, 0, 2 * fileSizeInKB * 1024);
                }
                else
                {
                    firstProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 1, 0, 0, fileSizeInKB * 1024);
                }

                // second progress checker
                if (DMLibTestContext.SourceType == DMLibDataType.Stream)
                {
                    // The destination is already created, will cause a transfer skip
                    secondProgressChecker = new ProgressChecker(2, fileSizeInKB * 1024, 0, 1 /* failed */, 1 /* skipped */, fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.Stream)
                {
                    secondProgressChecker = new ProgressChecker(2, 2 * fileSizeInKB * 1024, 1 /* transferred */, 1 /* failed */, 0, 2 * fileSizeInKB * 1024);
                }
                else if (DMLibTestContext.DestType == DMLibDataType.AppendBlob && (DMLibTestContext.CopyMethod != DMLibCopyMethod.ServiceSideAsyncCopy))
                {
                    secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 0, 1 /* failed */, 0, fileSizeInKB * 1024);
                }
                else
                {
                    secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 1 /* transferred */, 0, 0, fileSizeInKB * 1024);
                }

                // resume with firstResumeCheckpoint
                TransferItem resumeItem = transferItem.Clone();
                TransferContext resumeContext = null;

                if (IsStreamJournal)
                {
                    Exception deserializeEX = null;
                    try
                    {
                        resumeContext = new SingleTransferContext(journalStream)
                        {
                            ProgressHandler = firstProgressChecker.GetProgressHandler()
                        };
                    }
                    catch (Exception ex)
                    {
                        if ((DMLibTestContext.SourceType != DMLibDataType.Stream)
                            && (DMLibTestContext.DestType != DMLibDataType.Stream))
                        {
                            Test.Error("Should no exception in deserialization when no target is stream.");
                        }

                        deserializeEX = ex;
                    }
                }
                else
                {
                    resumeContext = new SingleTransferContext(IsStreamDirection() ? firstResumeCheckpoint : DMLibTestHelper.RandomReloadCheckpoint(firstResumeCheckpoint))
                    {
                        ProgressHandler = firstProgressChecker.GetProgressHandler()
                    };
                }

                resumeItem.TransferContext = resumeContext;

                result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibDataType.Local);
                destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

                Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");

                if (!IsStreamJournal)
                {
                    // resume with secondResumeCheckpoint
                    resumeItem = transferItem.Clone();
                    resumeContext = new SingleTransferContext(
                        IsStreamDirection() ? secondResumeCheckpoint : DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
                    {
                        ProgressHandler = secondProgressChecker.GetProgressHandler()
                    };
                    resumeItem.TransferContext = resumeContext;

                    result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                    destAdaptor = GetDestAdaptor(DMLibDataType.Local);
                    destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

                    Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                    Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
                }
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathDirectoryUpload()
        {
            int fileNum = 50;
            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(sourceDirectoryName, DMLibTestBase.FileName + "_" + (fileNum - 1).ToString(), pathLengthLimit));
            DMLibDataHelper.AddMultipleFiles(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileNum, fileSizeInKB);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongFilePathDirectoryDownload()
        {
            int fileNum = 50;
            int fileSizeInKB = 1;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddMultipleFiles(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileNum, fileSizeInKB);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(destDirectoryName, DMLibTestBase.FileName + "_" + (fileNum - 1).ToString(), pathLengthLimit));

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

            DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(DMLibDataType.Local);
            destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathResumeDirectoryUpload()
        {
            int bigFileSizeInKB = 5 * 1024; // 5 MB
            int smallFileSizeInKB = 1; // 1 KB
            int bigFileNum = 5;
            int smallFileNum = 50;
            long totalSizeInBytes = (bigFileSizeInKB * bigFileNum + smallFileSizeInKB * smallFileNum) * 1024;
            int totalFileNum = bigFileNum + smallFileNum;

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(sourceDirectoryName, LongPath.Combine("small", DMLibTestBase.FileName + "_" + (totalFileNum - 1).ToString()), pathLengthLimit));
            DirNode bigFileDirNode = new DirNode("big");
            DirNode smallFileDirNode = new DirNode("small");

            sourceDataInfo.RootNode.AddDirNode(bigFileDirNode);
            sourceDataInfo.RootNode.AddDirNode(smallFileDirNode);

            DMLibDataHelper.AddMultipleFiles(bigFileDirNode, FileName, bigFileNum, bigFileSizeInKB);
            DMLibDataHelper.AddMultipleFiles(smallFileDirNode, FileName, smallFileNum, smallFileSizeInKB);

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.LimitSpeed = true;
            options.IsDirectoryTransfer = true;

            using (Stream journalStream = new MemoryStream())
            {
                bool IsStreamJournal = random.Next(0, 2) == 0;
                var transferContext = IsStreamJournal ? new DirectoryTransferContext(journalStream) : new DirectoryTransferContext();
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
                    progressChecker.DataTransferred.WaitOne(30000);

                    if (!IsStreamJournal)
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

                if (!IsStreamJournal)
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

                if (IsStreamJournal)
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

                eventChecker.Reset();
                eventChecker.Apply(resumeContext);

                resumeItem.TransferContext = resumeContext;

                result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
                VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);
                VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

                if (!IsStreamJournal)
                {
                    // resume with secondResumeCheckpoint
                    resumeItem = transferItem.Clone();

                    progressChecker.Reset();
                    resumeContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler(),

                        // Need this overwrite callback since some files is already transferred to destination
                        ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY(),
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

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalDest)]
        public void LongFilePathResumeDirectoryDownload()
        {
            int bigFileSizeInKB = 5 * 1024; // 5 MB
            int smallFileSizeInKB = 1; // 1 KB
            int bigFileNum = 5;
            int smallFileNum = 50;
            long totalSizeInBytes = (bigFileSizeInKB * bigFileNum + smallFileSizeInKB * smallFileNum) * 1024;
            int totalFileNum = bigFileNum + smallFileNum;

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DirNode bigFileDirNode = new DirNode("big");
            DirNode smallFileDirNode = new DirNode("small");

            sourceDataInfo.RootNode.AddDirNode(bigFileDirNode);
            sourceDataInfo.RootNode.AddDirNode(smallFileDirNode);

            DMLibDataHelper.AddMultipleFiles(bigFileDirNode, FileName, bigFileNum, bigFileSizeInKB);
            DMLibDataHelper.AddMultipleFiles(smallFileDirNode, FileName, smallFileNum, smallFileSizeInKB);

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(destDirectoryName, LongPath.Combine("small", DMLibTestBase.FileName + "_" + (totalFileNum - 1).ToString()), pathLengthLimit));

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.LimitSpeed = true;
            options.IsDirectoryTransfer = true;
            options.DestTransferDataInfo = destDataInfo;
            options.DisableDestinationFetch = true;

            using (Stream journalStream = new MemoryStream())
            {
                bool IsStreamJournal = random.Next(0, 2) == 0;
                var transferContext = IsStreamJournal ? new DirectoryTransferContext(journalStream) : new DirectoryTransferContext();
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
                    progressChecker.DataTransferred.WaitOne(30000);

                    if (!IsStreamJournal)
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

                if (!IsStreamJournal)
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

                if (IsStreamJournal)
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

                eventChecker.Reset();
                eventChecker.Apply(resumeContext);

                resumeItem.TransferContext = resumeContext;

                result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
                VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);

                var destAdaptor = GetDestAdaptor(DMLibDataType.Local);
                destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

                Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");

                if (!IsStreamJournal)
                {
                    // resume with secondResumeCheckpoint
                    resumeItem = transferItem.Clone();

                    progressChecker.Reset();
                    resumeContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler(),

                        // Need this overwrite callback since some files is already transferred to destination
                        ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY(),
                    };

                    eventChecker.Reset();
                    eventChecker.Apply(resumeContext);

                    resumeItem.TransferContext = resumeContext;

                    result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

                    VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
                    VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);

                    destAdaptor = GetDestAdaptor(DMLibDataType.Local);
                    destDataInfo = destAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

                    Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                    Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
                }
            }
        }


        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathDirectoryUploadWith1KPath()
        {
            int fileNum = 50;
            int fileSizeInBytes = 1 * 1024;
            int relativePathLimit = 1 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(sourceDirectoryName);
            var baseFileName = LongPathExtension.Combine(sourceDataInfo.RootPath,
                GetRelativePathName(DMLibTestHelper.RandomNameSuffix(), fileNum, relativePathLimit));
            if(!LongPathDirectoryExtension.Exists(LongPathExtension.GetDirectoryName(baseFileName)))
            {
                LongPathDirectoryExtension.CreateDirectory(LongPathExtension.GetDirectoryName(baseFileName));
            }

            for (int i = 0; i < fileNum; ++i)
            {
                var fileName = baseFileName + "_" + i.ToString();
                Helper.GenerateFileInBytes(fileName, fileSizeInBytes);
            }
            sourceDataInfo = (SourceAdaptor as LocalDataAdaptor).GetTransferDataInfo(sourceDataInfo.RootPath);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            options.DisableDestinationFetch = true;
            options.DisableSourceGenerator = true;
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(destDirectoryName);
            var localDestAdaptor = GetDestAdaptor(DMLibDataType.Local);

            if ((DMLibTestContext.DestType & DMLibDataType.CloudBlob) != DMLibDataType.Unspecified)
            {
                var destAdaptor = DestAdaptor as CloudBlobDataAdaptor;
                CloudBlobDirectory blobDir = destAdaptor.BlobHelper.QueryBlobDirectory(destAdaptor.ContainerName, string.Empty);
                var downloadOptions = new DownloadDirectoryOptions();
                downloadOptions.Recursive = true;

                TransferManager.DownloadDirectoryAsync(blobDir, destDataInfo.RootPath, downloadOptions, null).Wait();
            }
            else if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
            {
                var destAdaptor = DestAdaptor as CloudFileDataAdaptor;
                CloudFileDirectory fileDir = destAdaptor.FileHelper.QueryFileDirectory(destAdaptor.ShareName, string.Empty);
                var downloadOptions = new DownloadDirectoryOptions();
                downloadOptions.Recursive = true;

                TransferManager.DownloadDirectoryAsync(fileDir, destDataInfo.RootPath, downloadOptions, null).Wait();
            }

            destDataInfo = localDestAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongFilePathDirectoryDownloadWith1KPath()
        {
            int fileNum = 50;
            int fileSizeInBytes = 1 * 1024;
            int relativePathLimit = 1 * 1024;
            DMLibDataInfo sourceLocalDataInfo = new DMLibDataInfo(sourceDirectoryName);
            var baseFileName = LongPathExtension.Combine(sourceLocalDataInfo.RootPath,
                GetRelativePathName(DMLibTestHelper.RandomNameSuffix(), fileNum, relativePathLimit));
            if (!LongPathDirectoryExtension.Exists(LongPathExtension.GetDirectoryName(baseFileName)))
            {
                LongPathDirectoryExtension.CreateDirectory(LongPathExtension.GetDirectoryName(baseFileName));
            }

            for (int i = 0; i < fileNum; ++i)
            {
                var fileName = baseFileName + "_" + i.ToString();
                Helper.GenerateFileInBytes(fileName, fileSizeInBytes);
            }

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            options.DisableDestinationFetch = true;
            options.DisableSourceGenerator = true;
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            DMLibDataInfo destDataInfo = new DMLibDataInfo(destDirectoryName);
            options.DestTransferDataInfo = destDataInfo;

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(sourceLocalDataInfo.RootPath);
            // Prepare data
            this.CleanupData(true, true);
            options.DisableSourceCleaner = true;
            if ((DMLibTestContext.SourceType & DMLibDataType.CloudBlob) != DMLibDataType.Unspecified)
            {
                var sourceAdaptor = SourceAdaptor as CloudBlobDataAdaptor;
                sourceAdaptor.BlobHelper.BlobClient.GetContainerReference(sourceAdaptor.ContainerName).CreateIfNotExists();
                CloudBlobDirectory blobDir = sourceAdaptor.BlobHelper.QueryBlobDirectory(sourceAdaptor.ContainerName, string.Empty);
                var uploadOptions = new UploadDirectoryOptions();
                uploadOptions.Recursive = true;
                switch(DMLibTestContext.SourceType)
                {
                    case DMLibDataType.BlockBlob:
                        uploadOptions.BlobType = BlobType.BlockBlob;
                        break;
                    case DMLibDataType.AppendBlob:
                        uploadOptions.BlobType = BlobType.AppendBlob;
                        break;
                    case DMLibDataType.PageBlob:
                        uploadOptions.BlobType = BlobType.PageBlob;
                        break;
                }

                TransferManager.UploadDirectoryAsync(sourceLocalDataInfo.RootPath, blobDir, uploadOptions, null).Wait();

                sourceDataInfo = sourceAdaptor.GetTransferDataInfo(string.Empty);
            }
            else if (DMLibTestContext.SourceType == DMLibDataType.CloudFile)
            {

                var sourceAdaptor = SourceAdaptor as CloudFileDataAdaptor;
                sourceAdaptor.FileHelper.FileClient.GetShareReference(sourceAdaptor.ShareName).CreateIfNotExists();
                CloudFileDirectory fileDir = sourceAdaptor.FileHelper.QueryFileDirectory(sourceAdaptor.ShareName, string.Empty);
                var uploadOptions = new UploadDirectoryOptions();
                uploadOptions.Recursive = true;

                TransferManager.UploadDirectoryAsync(sourceLocalDataInfo.RootPath, fileDir, uploadOptions, null).Wait();

                sourceDataInfo = sourceAdaptor.GetTransferDataInfo(string.Empty);
            }

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            var localDestAdaptor = GetDestAdaptor(DMLibDataType.Local);
            destDataInfo = localDestAdaptor.GetTransferDataInfo(destDataInfo.RootPath);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, destDataInfo), "Verify transfer result.");
        }


        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathDirectoryShouldTransfer()
        {
            // Prepare data
            int totalFileNumber = DMLibTestConstants.FlatFileCount;
            int expectedTransferred = totalFileNumber, transferred = 0;
            int expectedSkipped = 0, skipped = 0;
            int expectedFailed = 0, failed = 0;
            int pathLengthLimit = random.Next(261, this.pathLengthLimit);
            DMLibDataInfo sourceDataInfo = this.GenerateSourceDataInfo(FileNumOption.FlatFolder, 1024, GetDirectoryName(sourceDirectoryName, DMLibTestBase.FileName + "_" + (totalFileNumber - 1).ToString(), pathLengthLimit));

            DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

            List<String> notTransferredFileNames = new List<String>();
            dirTransferContext.ShouldTransferCallbackAsync = async (source, dest) =>
            {
                return await Task.Run(() =>
                {
                    if (Helper.RandomBoolean())
                    {
                        return true;
                    }
                    else
                    {
                        Interlocked.Decrement(ref expectedTransferred);
                        string fullName = DMLibTestHelper.TransferInstanceToString(source);
                        string fileName = fullName.Substring(fullName.IndexOf(DMLibTestBase.FileName));
                        lock (notTransferredFileNames)
                        {
                            notTransferredFileNames.Add(fileName);
                        }
                        Test.Info("{0} is filterred in ShouldTransfer.", fileName);
                        return false;
                    }
                });
            };

            dirTransferContext.FileTransferred += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref transferred);
            };

            dirTransferContext.FileSkipped += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref skipped);
            };

            dirTransferContext.FileFailed += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref failed);
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            options.DisableSourceCleaner = true;

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = dirTransferContext;

                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            // Execute test case
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // Verify result
            DMLibDataInfo expectedDataInfo = sourceDataInfo.Clone();
            DirNode expectedRootNode = expectedDataInfo.RootNode;
            foreach (string fileNames in notTransferredFileNames)
            {
                expectedRootNode.DeleteFileNode(fileNames);
            }

            VerificationHelper.VerifyTransferSucceed(result, expectedDataInfo);
            Test.Assert(expectedTransferred == transferred, string.Format("Verify transferred number. Expected: {0}, Actual: {1}", expectedTransferred, transferred));
            Test.Assert(expectedSkipped == skipped, string.Format("Verify skipped number. Expected: {0}, Actual: {1}", expectedSkipped, skipped));
            Test.Assert(expectedFailed == failed, string.Format("Verify failed number. Expected: {0}, Actual: {1}", expectedFailed, failed));
        }

        private static string GetTransferString(DMLibDataType sourceType, DMLibDataType destType, bool isAsync)
        {
            return sourceType.ToString() + destType.ToString() + (isAsync ? "async" : "");
        }

        private static bool IsStreamDirection()
        {
            return DMLibTestContext.DestType == DMLibDataType.Stream || DMLibTestContext.SourceType == DMLibDataType.Stream;
        }

        private static string GetDirectoryName(string path, string fileName, int length)
        {
            int nameLimit = 240;
            string tempName = "t";
#if DOTNET5_4
            string tempPath = LongPathExtension.Combine(LongPathExtension.Combine(path, tempName), fileName);
            if(CrossPlatformHelpers.IsWindows)
            {
                tempPath = LongPathExtension.ToUncPath(tempPath);
            }
#else
            string tempPath = LongPathExtension.ToUncPath(LongPathExtension.Combine(LongPathExtension.Combine(path, tempName), fileName));
#endif
            int targetLength = length - tempPath.Length + tempName.Length;
            string middleDirectoryName = "";
            while(targetLength > 0)
            {
                if (targetLength == nameLimit + 1)
                {
                    middleDirectoryName += new string('t', nameLimit / 2) + Path.DirectorySeparatorChar;
                    targetLength -= nameLimit / 2 + 1;
                    continue;
                }

                if (targetLength > nameLimit)
                {
                    middleDirectoryName += new string('t', nameLimit) + Path.DirectorySeparatorChar;
                    targetLength -= nameLimit + 1;
                }
                else
                {
                    middleDirectoryName += new string('t', targetLength);
                    targetLength = 0;
                }
            }
            return LongPathExtension.Combine(path, middleDirectoryName);
        }

        private static string GetRelativePathName(string prefix, int fileNum,int length)
        {
            int nameLimit = 240;
            string tempName = "t";
#if DOTNET5_4
            string tempPath = LongPathExtension.Combine(LongPathExtension.Combine(tempName, prefix), "_" + (fileNum-1).ToString());
            if(CrossPlatformHelpers.IsWindows)
            {
                tempPath = LongPathExtension.ToUncPath(tempPath);
            }
#else
            string tempPath = LongPathExtension.ToUncPath(LongPathExtension.Combine(LongPathExtension.Combine(tempName, prefix), "_" + (fileNum-1).ToString()));
#endif
            int targetLength = length - tempPath.Length + tempName.Length;
            string middleDirectoryName = "";
            while(targetLength > 0)
            {
                if (targetLength == nameLimit + 1)
                {
                    middleDirectoryName += new string('t', nameLimit / 2) + Path.DirectorySeparatorChar;
                    targetLength -= nameLimit / 2 + 1;
                    continue;
                }

                if (targetLength > nameLimit)
                {
                    middleDirectoryName += new string('t', nameLimit) + Path.DirectorySeparatorChar;
                    targetLength -= nameLimit + 1;
                }
                else
                {
                    middleDirectoryName += new string('t', targetLength);
                    targetLength = 0;
                }
            }
            return LongPathExtension.Combine(middleDirectoryName, prefix);
        }
    }
}
