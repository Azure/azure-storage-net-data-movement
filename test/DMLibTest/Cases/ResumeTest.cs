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
    {
        #region Additional test attributes
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
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
        [DMLibTestMethodSet(DMLibTestMethodSet.AllSync)]
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
            var progressChecker = new ProgressChecker(1, fileSizeInKB * 1024);
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
                    Thread.Sleep(1000);

                    // Cancel the transfer and store the second checkpoint
                    tokenSource.Cancel();
                    secondCheckpoint = transferContext.LastCheckpoint;
                };

            // Cancel and store checkpoint for resume
            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 1, "Verify job is cancelled");
            Exception exception = result.Exceptions[0];
            VerificationHelper.VerifyExceptionErrorMessage(exception, "A task was canceled.");

            TransferCheckpoint firstResumeCheckpoint = null, secondResumeCheckpoint = null;

            // DMLib doesn't support to resume transfer from a checkpoint which is inconsistent with
            // the actual transfer progress when the destination is an append blob.
            if (Helper.RandomBoolean() && DMLibTestContext.DestType != DMLibDataType.AppendBlob)
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

            // resume with firstResumeCheckpoint
            TransferItem resumeItem = transferItem.Clone();
            progressChecker.Reset();
            TransferContext resumeContext = new TransferContext(firstResumeCheckpoint)
            {
                ProgressHandler = progressChecker.GetProgressHandler()
            };
            resumeItem.TransferContext = resumeContext;

            result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

            VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);

            // resume with secondResumeCheckpoint
            resumeItem = transferItem.Clone();
            progressChecker.Reset();
            resumeContext = new TransferContext(secondResumeCheckpoint)
            {
                ProgressHandler = progressChecker.GetProgressHandler()
            };
            resumeItem.TransferContext = resumeContext;

            result = this.RunTransferItems(new List<TransferItem>() { resumeItem }, new TestExecutionOptions<DMLibDataInfo>());

            if (DMLibTestContext.DestType != DMLibDataType.AppendBlob || DMLibTestContext.SourceType == DMLibDataType.Stream)
            {
                VerificationHelper.VerifySingleObjectResumeResult(result, sourceDataInfo);
            }
            else
            {
                Test.Assert(result.Exceptions.Count == 1, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                exception = result.Exceptions[0];
                Test.Assert(exception is InvalidOperationException, "Verify reumse fails when checkpoint is inconsistent with the actual progress when destination is append blob.");
                VerificationHelper.VerifyExceptionErrorMessage(exception, "Destination might be changed by other process or application.");
            }
        }
    }
}
