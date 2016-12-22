//------------------------------------------------------------------------------
// <copyright file="SnapshotTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using System.Threading;

    [MultiDirectionTestClass]
    public class SnapshotTest : DMLibTestBase
#if DNXCORE50
        , System.IDisposable
#endif
    {
        #region Initialization and cleanup methods
#if DNXCORE50
        public SnapshotTest()
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
            Test.Info("Class Initialize: SnapshotTest");
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
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        public void TestDirectoryIncludeSnapshots()
        {
            string snapshotFile1 = "snapshotFile1";
            string snapshotFile2 = "snapshotFile2";
            string snapshotFile3 = "snapshotFile3";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            // the 1st file has 1 snapshot
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile1, 1024);
            FileNode fileNode1 = sourceDataInfo.RootNode.GetFileNode(snapshotFile1);
            fileNode1.SnapshotsCount = 1;

            // the 2nd file has 2 snapshots
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile2, 1024);
            FileNode fileNode2 = sourceDataInfo.RootNode.GetFileNode(snapshotFile2);
            fileNode2.SnapshotsCount = 2;

            // the 3rd file has no snapshot
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile3, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            // transfer with IncludeSnapshots = true
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic dirOptions = DefaultTransferDirectoryOptions;
                dirOptions.Recursive = true;
                dirOptions.IncludeSnapshots = true;
                transferItem.Options = dirOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // verify that snapshots are transferred
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        public void TestDirectoryNotIncludeSnapshots()
        {
            string snapshotFile1 = "snapshotFile1";
            string snapshotFile2 = "snapshotFile2";
            string snapshotFile3 = "snapshotFile3";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile1, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile2, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, snapshotFile3, 1024);

            // the 1st file has 1 snapshot
            FileNode fileNode1 = sourceDataInfo.RootNode.GetFileNode(snapshotFile1);
            fileNode1.SnapshotsCount = 1;

            // the 2nd file has 2 snapshots
            FileNode fileNode2 = sourceDataInfo.RootNode.GetFileNode(snapshotFile2);
            fileNode2.SnapshotsCount = 2;

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            // transfer with default options, or IncludeSnapshots = false
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic dirOptions = DefaultTransferDirectoryOptions;
                dirOptions.Recursive = true;

                if (random.Next(0, 2) == 0)
                {
                    Test.Info("Transfer directory with IncludeSnapshots=false");
                    dirOptions.IncludeSnapshots = false;
                }
                else
                {
                    Test.Info("Transfer directory with defaullt IncludeSnapshots");
                }

                transferItem.Options = dirOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // verify that only non-snapshot blobs are transferred

            DMLibDataInfo expectedDataInfo = new DMLibDataInfo(string.Empty);
            expectedDataInfo.RootNode.AddFileNode(fileNode1);
            expectedDataInfo.RootNode.AddFileNode(fileNode2);
            expectedDataInfo.RootNode.AddFileNode(sourceDataInfo.RootNode.GetFileNode(snapshotFile3));
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        public void TestDirectoryWithSpecialCharNamedBlobs()
        {
#if DNXCORE50
            // TODO: There's a known issue that signature for URI with '[' or ']' doesn't work.
            string specialChars = "~`!@#$%()-_+={};?.^&";
#else
            string specialChars = "~`!@#$%()-_+={}[];?.^&";
#endif

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);

            for (int i = 0; i < specialChars.Length; ++i)
            {
                string fileName = DMLibTestBase.FileName + specialChars[i] + DMLibTestBase.FileName;

                // the 1st file has 1 snapshot
                DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, fileName, 1024);
                FileNode fileNode1 = sourceDataInfo.RootNode.GetFileNode(fileName);
                fileNode1.SnapshotsCount = 1;
            }

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            // transfer with IncludeSnapshots = true
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic dirOptions = DefaultTransferDirectoryOptions;
                dirOptions.Recursive = true;
                dirOptions.IncludeSnapshots = true;
                transferItem.Options = dirOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // verify that snapshots are transferred
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }
    }
}
