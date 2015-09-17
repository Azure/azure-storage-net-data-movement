//------------------------------------------------------------------------------
// <copyright file="VerificationHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using DMLibTestCodeGen;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public static class VerificationHelper
    {
        public static void VerifySingleObjectResumeResult(TestResult<DMLibDataInfo> result, DMLibDataInfo expectedDataInfo)
        {
            if (DMLibTestContext.SourceType != DMLibDataType.Stream && DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");
            }
            else
            {
                Test.Assert(result.Exceptions.Count == 1, "Verify stream resume is not supported");
                Exception exception = result.Exceptions[0];
                Test.Assert(exception is NotSupportedException, "Verify stream resume is not supported");
            }
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
    }
}
