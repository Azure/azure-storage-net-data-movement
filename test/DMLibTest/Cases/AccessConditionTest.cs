//------------------------------------------------------------------------------
// <copyright file="AccessConditionTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Cases
{
    using System;
    using System.Net;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class AccessConditionTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public AccessConditionTest()
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
            Test.Info("Class Initialize: AccessConditionTest");
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
        [DMLibTestMethodSet(DMLibTestMethodSet.CloudBlobSource)]
        public void TestSourceAccessCondition()
        {
            this.TestAccessCondition(SourceOrDest.Source);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.CloudBlobDest)]
        public void TestDestAccessCondition()
        {
            this.TestAccessCondition(SourceOrDest.Dest);
        }

        private void TestAccessCondition(SourceOrDest sourceOrDest)
        {
            string eTag = "\"notmatch\"";
            AccessCondition accessCondition = new AccessCondition()
            {
                IfMatchETag = eTag
            };

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, DMLibTestBase.FileName, 1024);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, DMLibTestBase.FileName, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();

            if (sourceOrDest == SourceOrDest.Dest)
            {
                options.DestTransferDataInfo = destDataInfo;
            }

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;

                if (sourceOrDest == SourceOrDest.Source)
                {
                    transferOptions.SourceAccessCondition = accessCondition;
                }
                else
                {
                    transferOptions.DestinationAccessCondition = accessCondition;
                }

                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            if (sourceOrDest == SourceOrDest.Dest)
            {
                Test.Assert(DMLibDataHelper.Equals(destDataInfo, result.DataInfo), "Verify no file is transferred.");
            }
            else
            {
                if (DMLibTestContext.DestType != DMLibDataType.Stream)
                {
                    Test.Assert(DMLibDataHelper.Equals(new DMLibDataInfo(string.Empty), result.DataInfo), "Verify no file is transferred.");
                }
                else
                {
                    foreach(var fileNode in result.DataInfo.EnumerateFileNodes())
                    {
                        Test.Assert(fileNode.SizeInByte == 0, "Verify file {0} is empty", fileNode.Name);
                    }
                }
            }
            
            // Verify TransferException
            if (result.Exceptions.Count != 1)
            {
                Test.Error("There should be exactly one exceptions.");
                return;
            }

            Exception exception = result.Exceptions[0];
#if DOTNET5_4
            VerificationHelper.VerifyTransferException(exception, TransferErrorCode.Unknown);
            
            // Verify innner StorageException
            VerificationHelper.VerifyStorageException(exception.InnerException, (int)HttpStatusCode.PreconditionFailed,
                "The condition specified using HTTP conditional header(s) is not met.");
#else
            VerificationHelper.VerifyTransferException(exception, TransferErrorCode.Unknown);

            // Verify innner StorageException
            VerificationHelper.VerifyStorageException(exception.InnerException, (int)HttpStatusCode.PreconditionFailed,
                "The condition specified using HTTP conditional header(s) is not met.");
#endif
        }
    }
}
