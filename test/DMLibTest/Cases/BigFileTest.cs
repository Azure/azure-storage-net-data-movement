//------------------------------------------------------------------------------
// <copyright file="BigFileTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class BigFileTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public BigFileTest()
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
            Test.Info("Class Initialize: BigFileTest");
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
        public void TransferBigSizeObject()
        {
            int originParallel = TransferManager.Configurations.ParallelOperations;
            TransferManager.Configurations.ParallelOperations = 4;
            try
            {
                DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
                DMLibDataHelper.AddMultipleFilesBigSize(sourceDataInfo.RootNode, DMLibTestBase.FileName);

                var option = new TestExecutionOptions<DMLibDataInfo>();
                var result = this.ExecuteTestCase(sourceDataInfo, option);

                Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
                Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");

                if (!(DMLibTestContext.DestType == DMLibDataType.Local || DMLibTestContext.DestType == DMLibDataType.Stream))
                {
                    this.ValidateDestinationMD5ByDownloading(result.DataInfo, option);
                }
            }
            finally
            {
                TransferManager.Configurations.ParallelOperations = originParallel;
            }
        }
    }
}
