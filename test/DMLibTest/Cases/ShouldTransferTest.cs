//------------------------------------------------------------------------------
// <copyright file="ShouldTransferTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    [MultiDirectionTestClass]
    public class ShouldTransferTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public ShouldTransferTest()
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
            Test.Info("Class Initialize: ShouldTransferTest");
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
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void DirectoryShouldTransfer()
        {
            // Prepare data
            int totaFileNumber = DMLibTestConstants.FlatFileCount;
            int expectedTransferred = totaFileNumber, transferred = 0;
            int expectedSkipped = 0, skipped = 0;
            int expectedFailed = 0, failed = 0;
            DMLibDataInfo sourceDataInfo = this.GenerateSourceDataInfo(FileNumOption.FlatFolder, 1024);

            DirectoryTransferContext dirTransferContext = new DirectoryTransferContext();

            List<String> notTransferredFileNames = new List<String>();
            dirTransferContext.ShouldTransferCallbackAsync = async (source, dest) =>
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
            foreach(string fileNames in notTransferredFileNames)
            {
                expectedRootNode.DeleteFileNode(fileNames);
            }

            VerificationHelper.VerifyTransferSucceed(result, expectedDataInfo);
            Test.Assert(expectedTransferred == transferred, string.Format("Verify transferred number. Expected: {0}, Actual: {1}", expectedTransferred, transferred));
            Test.Assert(expectedSkipped == skipped, string.Format("Verify skipped number. Expected: {0}, Actual: {1}", expectedSkipped, skipped));
            Test.Assert(expectedFailed == failed, string.Format("Verify failed number. Expected: {0}, Actual: {1}", expectedFailed, failed));
        }
    }
}
