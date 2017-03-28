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
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using DMLibTest.Framework;
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
        private int pathLengthLimit = 32 * 1000;

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
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalSource)]
        public void LongFilePathSingleUpload()
        {
            int fileSizeInKB = 1 * 1024;
            string directoryName = Directory.GetCurrentDirectory();
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(directoryName, DMLibTestBase.FileName, pathLengthLimit));
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            var options = new TestExecutionOptions<DMLibDataInfo>();

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void LongFilePathSingleDownload()
        {
            int fileSizeInKB = 1 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            string directoryName = Directory.GetCurrentDirectory();
            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(directoryName, DMLibTestBase.FileName, pathLengthLimit));
            DMLibDataHelper.AddOneFile(destDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

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
            string directoryName = Directory.GetCurrentDirectory();
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(GetDirectoryName(directoryName, DMLibTestBase.FileName, pathLengthLimit));
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
                    progressChecker.DataTransferred.WaitOne();

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
                VerificationHelper.VerifyExceptionErrorMessage(exception, "A task was canceled.");

                TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;
                ProgressChecker firstProgressChecker = null, secondProgressChecker = null;

                if (!IsStreamJournal)
                {
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
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalDest)]
        public void LongFilePathResumeSingleDownload()
        {
            int fileSizeInKB = 100 * 1024;
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

            string directoryName = Directory.GetCurrentDirectory();
            DMLibDataInfo destDataInfo = new DMLibDataInfo(GetDirectoryName(directoryName, DMLibTestBase.FileName, pathLengthLimit));
            DMLibDataHelper.AddOneFile(destDataInfo.RootNode, DMLibTestBase.FileName, fileSizeInKB);

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
                    progressChecker.DataTransferred.WaitOne();

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
                VerificationHelper.VerifyExceptionErrorMessage(exception, "A task was canceled.");

                TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;
                ProgressChecker firstProgressChecker = null, secondProgressChecker = null;

                if (!IsStreamJournal)
                {
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
            int nameLimit = 200;
            string tempName = "t";
            string tempPath = LongPathExtention.ToUncPath(LongPathExtention.Combine(LongPathExtention.Combine(path, tempName), fileName));
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
            return LongPathExtention.Combine(path, middleDirectoryName);
        }
    }
}
