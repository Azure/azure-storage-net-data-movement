//------------------------------------------------------------------------------
// <copyright file="VerificationHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public static class VerificationHelper
    {
        public static void VerifyTransferSucceed(TestResult<DMLibDataInfo> result, DMLibDataInfo expectedDataInfo)
        {
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");
        }
        
        public static void VerifySingleObjectResumeResult(TestResult<DMLibDataInfo> result, DMLibDataInfo expectedDataInfo)
        {
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");
        }

        public static void VerifyTransferException(Exception exception, TransferErrorCode expectedErrorCode, params string[] expectedMessages)
        {
            TransferException transferException = exception as TransferException;
            if (transferException == null)
            {
                Test.Error("Verify exception is a transfer exception.");
                return;
            }

            Test.Assert(transferException.ErrorCode == expectedErrorCode, "Verify error code: {0}, expected: {1}", transferException.ErrorCode, expectedErrorCode);
            VerificationHelper.VerifyExceptionErrorMessage(exception, expectedMessages);
        }

        public static void VerifyStorageException(Exception exception, int expectedHttpStatusCode, params string[] expectedMessages)
        {
            StorageException storageException = exception as StorageException;
            if (storageException == null)
            {
                Test.Error("Verify exception is a storage exception.");
                return;
            }

            Test.Assert(storageException.RequestInformation.HttpStatusCode == expectedHttpStatusCode, "Verify http status code: {0}, expected: {1}", storageException.RequestInformation.HttpStatusCode, expectedHttpStatusCode);
            VerificationHelper.VerifyExceptionErrorMessage(exception, expectedMessages);
        }

        public static void VerifyExceptionErrorMessage(Exception exception, params string[] expectedMessages)
        {
            Test.Info("Error message: {0}", exception.Message);

            foreach (string expectedMessage in expectedMessages)
            {
                Test.Assert(exception.Message.Contains(expectedMessage), "Verify exception message contains {0}", expectedMessage);
            }
        }

        public static void VerifyFinalProgress(ProgressChecker progressChecker, long? transferredFilesNum, long? skippedFilesNum, long? failedFilesNum)
        {
            if (transferredFilesNum != null)
            {
                Test.Assert(progressChecker.TransferredFilesNumber == (int)transferredFilesNum, "Verify transferred files number: expected {0}, actual {1}.", transferredFilesNum, progressChecker.TransferredFilesNumber);
            }

            if (skippedFilesNum != null)
            {
                Test.Assert(progressChecker.SkippedFilesNumber == (int)skippedFilesNum, "Verify skipped files number: expected {0}, actual {1}.", skippedFilesNum, progressChecker.SkippedFilesNumber);
            }

            if (failedFilesNum != null)
            {
                Test.Assert(progressChecker.FailedFilesNumber == (int)failedFilesNum, "Verify failed files number: expected {0}, actual {1}.", failedFilesNum, progressChecker.FailedFilesNumber);
            }
        }

        public static void VerifySingleTransferStatus(TestResult<DMLibDataInfo> result, long? transferredFilesNum, long? skippedFilesNum, long? failedFilesNum, long? transferredBytes)
        {
            if (result.TransferItems.Count != 1)
            {
                Test.Error(string.Format("Expect exact one transfer item. Actaul {0}", result.TransferItems.Count));
                return;
            }

            TransferStatus status = result.TransferItems[0].FinalStatus;

            if (status == null)
            {
                Test.Error("Transfer status should not be null");
                return;
            }

            if (transferredFilesNum != null)
            {
                Test.Assert(status.NumberOfFilesTransferred == transferredFilesNum, "Verify transferred files number: expected {0}, actual {1}.", transferredFilesNum, status.NumberOfFilesTransferred);
            }

            if (skippedFilesNum != null)
            {
                Test.Assert(status.NumberOfFilesSkipped == skippedFilesNum, "Verify skipped files number: expected {0}, actual {1}.", skippedFilesNum, status.NumberOfFilesSkipped);
            }

            if (failedFilesNum != null)
            {
                Test.Assert(status.NumberOfFilesFailed == failedFilesNum, "Verify failed files number: expected {0}, actual {1}.", failedFilesNum, status.NumberOfFilesFailed);
            }

            if (transferredBytes != null)
            {
                Test.Assert(status.BytesTransferred == transferredBytes, "Verify transferred bytes: expected {0}, actual {1}", transferredBytes, status.BytesTransferred);
            }
        }
    }
}
