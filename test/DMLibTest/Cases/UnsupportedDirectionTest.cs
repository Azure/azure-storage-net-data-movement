//------------------------------------------------------------------------------
// <copyright file="UnsupportedDirectionTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Cases
{
    using System;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class UnsupportedDirectionTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods
#if DNXCORE50
        public UnsupportedDirectionTest()
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
            Test.Info("Class Initialize: UnsupportedDirectionTest");
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
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.CloudBlob & ~DMLibDataType.BlockBlob)]
        [DMLibTestMethod(DMLibDataType.AppendBlob, DMLibDataType.CloudBlob & ~DMLibDataType.AppendBlob)]
        [DMLibTestMethod(DMLibDataType.PageBlob, DMLibDataType.CloudBlob & ~DMLibDataType.PageBlob)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.CloudBlob & ~DMLibDataType.BlockBlob, isAsync: true)]
        [DMLibTestMethod(DMLibDataType.AppendBlob, DMLibDataType.CloudBlob & ~DMLibDataType.AppendBlob, isAsync: true)]
        [DMLibTestMethod(DMLibDataType.PageBlob, DMLibDataType.CloudBlob & ~DMLibDataType.PageBlob, isAsync: true)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.PageBlob, isAsync: true)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.AppendBlob, isAsync: true)]
        [DMLibTestMethod(DMLibDataType.URI, DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.URI, DMLibDataType.CloudBlob)]
        public void TestUnsupportedDirection()
        {
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, DMLibTestBase.FileName, 1024);

            var result = this.ExecuteTestCase(sourceDataInfo, new TestExecutionOptions<DMLibDataInfo>());

            Test.Assert(result.Exceptions.Count == 1, "Verify no exception is thrown.");

            Exception exception = result.Exceptions[0];

            if (DMLibTestContext.SourceType == DMLibDataType.URI)
            {
                Test.Assert(exception is NotSupportedException, "Verify exception is NotSupportedException.");
                if (DMLibTestContext.DestType == DMLibDataType.CloudFile)
                {
                    VerificationHelper.VerifyExceptionErrorMessage(exception, "Copying from uri to Azure File Storage synchronously is not supported");
                }
                else
                {
                    VerificationHelper.VerifyExceptionErrorMessage(exception, "Copying from uri to Azure Blob Storage synchronously is not supported");
                }
            }
            else if (DMLibTestBase.IsCloudBlob(DMLibTestContext.SourceType) && DMLibTestBase.IsCloudBlob(DMLibTestContext.DestType))
            {
                Test.Assert(exception is InvalidOperationException, "Verify exception is InvalidOperationException.");
                VerificationHelper.VerifyExceptionErrorMessage(exception, "Blob type of source and destination must be the same.");
            }
            else
            {
                Test.Assert(exception is InvalidOperationException, "Verify exception is InvalidOperationException.");
                VerificationHelper.VerifyExceptionErrorMessage(exception, 
                    string.Format("Copying from File Storage to {0} Blob Storage asynchronously is not supported.", MapBlobDataTypeToBlobType(DMLibTestContext.DestType)));
            }

            Test.Assert(DMLibDataHelper.Equals(new DMLibDataInfo(string.Empty), result.DataInfo), "Verify no file is transfered.");
        }
    }
}
