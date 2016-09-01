//------------------------------------------------------------------------------
// <copyright file="ResumeTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class ResumeTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public ResumeTest()
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
            Test.Info("Class Initialize: ResumeTest");
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
        public void TestResume()
        {
            int fileSizeInKB = 100 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.LimitSpeed = true;
            var transferContext = new TransferContext();
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
                    // Wait until there are data transferred
                    progressChecker.DataTransferred.WaitOne();

                    // Store the first checkpoint
                    firstCheckpoint = transferContext.LastCheckpoint;

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                };

            // Cancel and store checkpoint for resume
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            secondCheckpoint = transferContext.LastCheckpoint;

            Test.Assert(result.Exceptions.Count == 1, "Verify job is cancelled");
            Exception exception = result.Exceptions[0];
            VerificationHelper.VerifyExceptionErrorMessage(exception, "A task was canceled.");

            TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;
            ProgressChecker firstProgressChecker = null, secondProgressChecker = null;

            // DMLib doesn't support to resume transfer from a checkpoint which is inconsistent with
            // the actual transfer progress when the destination is an append blob.
            if (Helper.RandomBoolean() && (DMLibTestContext.DestType != DMLibDataType.AppendBlob || DMLibTestContext.IsAsync))
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
            else if (DMLibTestContext.DestType == DMLibDataType.AppendBlob && !DMLibTestContext.IsAsync)
            {
                secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 0, 1 /* failed */, 0, fileSizeInKB * 1024);
            }
            else
            {
                secondProgressChecker = new ProgressChecker(1, fileSizeInKB * 1024, 1 /* transferred */, 0, 0, fileSizeInKB * 1024);
            }

            // resume with firstResumeCheckpoint
            TransferItem resumeItem = transferItem.Clone();
            TransferContext resumeContext = new TransferContext(
                IsStreamDirection() ? firstResumeCheckpoint : DMLibTestHelper.RandomReloadCheckpoint(firstResumeCheckpoint))
            {
                ProgressHandler = firstProgressChecker.GetProgressHandler()
            };

            resumeItem.TransferContext = resumeContext;

            result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

            if (DMLibTestContext.SourceType == DMLibDataType.Stream && DMLibTestContext.DestType != DMLibDataType.BlockBlob)
            {
                Test.Assert(result.Exceptions.Count == 1, "Verify transfer is skipped when source is stream.");
                exception = result.Exceptions[0];
                VerificationHelper.VerifyTransferException(result.Exceptions[0], TransferErrorCode.NotOverwriteExistingDestination, "Skiped file");
            }
            else
            {
                // For sync copy, recalculate md5 of destination by downloading the file to local.
                if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
                {
                    DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
                }

                VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);
            }

            // resume with secondResumeCheckpoint
            resumeItem = transferItem.Clone();
            resumeContext = new TransferContext(
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
                VerificationHelper.VerifyTransferException(result.Exceptions[0], TransferErrorCode.NotOverwriteExistingDestination, "Skiped file");
            }
            else if (DMLibTestContext.DestType == DMLibDataType.AppendBlob && !DMLibTestContext.IsAsync)
            {
                Test.Assert(result.Exceptions.Count == 1, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                exception = result.Exceptions[0];
                Test.Assert(exception is InvalidOperationException, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                VerificationHelper.VerifyExceptionErrorMessage(exception, "Destination might be changed by other process or application.");
            }
            else
            {
                // For sync copy, recalculate md5 of destination by downloading the file to local.
                if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
                {
                    DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
                }

                VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void TestDirectoryResume()
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

            TransferItem transferItem = null;
            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.LimitSpeed = true;
            options.IsDirectoryTransfer = true;

            var transferContext = new TransferContext();
            var progressChecker = new ProgressChecker(totalFileNum, totalSizeInBytes, totalFileNum, null, 0, totalSizeInBytes);
            transferContext.ProgressHandler = progressChecker.GetProgressHandler();
            var eventChecker = new TransferEventChecker();
            eventChecker.Apply(transferContext);

            transferContext.FileFailed += (sender, e) =>
            {
                Test.Assert(e.Exception.Message.Contains("cancel"), "Verify task is canceled: {0}", e.Exception.Message);
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

                // Store the first checkpoint
                firstCheckpoint = transferContext.LastCheckpoint;
                Thread.Sleep(100);

                // Cancel the transfer and store the second checkpoint
                tokenSource.Cancel();
            };

            // Cancel and store checkpoint for resume
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            secondCheckpoint = transferContext.LastCheckpoint;

            if (progressChecker.FailedFilesNumber <= 0)
            {
                Test.Error("Verify file number in progress. Failed: {0}", progressChecker.FailedFilesNumber);
            }

            TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;

            Test.Info("Resume with the second checkpoint first.");
            firstResumeCheckpoint = secondCheckpoint;
            secondResumeCheckpoint = firstCheckpoint;

            // resume with firstResumeCheckpoint
            TransferItem resumeItem = transferItem.Clone();

            progressChecker.Reset();
            TransferContext resumeContext = new TransferContext(DMLibTestHelper.RandomReloadCheckpoint(firstResumeCheckpoint))
            {
                ProgressHandler = progressChecker.GetProgressHandler()
            };

            eventChecker.Reset();
            eventChecker.Apply(resumeContext);

            resumeItem.TransferContext = resumeContext;

            result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

            VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
            VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);
            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);

            // resume with secondResumeCheckpoint
            resumeItem = transferItem.Clone();

            progressChecker.Reset();
            resumeContext = new TransferContext(DMLibTestHelper.RandomReloadCheckpoint(secondResumeCheckpoint))
            {
                ProgressHandler = progressChecker.GetProgressHandler(),

                // Need this overwrite callback since some files is already transferred to destination
                OverwriteCallback = DMLibInputHelper.GetDefaultOverwiteCallbackY(),
            };

            eventChecker.Reset();
            eventChecker.Apply(resumeContext);

            resumeItem.TransferContext = resumeContext;

            result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

            VerificationHelper.VerifyFinalProgress(progressChecker, totalFileNum, 0, 0);
            VerificationHelper.VerifySingleTransferStatus(result, totalFileNum, 0, 0, totalSizeInBytes);
            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
        }

        private static bool IsStreamDirection()
        {
            return DMLibTestContext.DestType == DMLibDataType.Stream || DMLibTestContext.SourceType == DMLibDataType.Stream;
        }
    }
}
