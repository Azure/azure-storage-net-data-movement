//------------------------------------------------------------------------------
// <copyright file="SnapshotTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;
    using MS.Test.Common.MsTestLib;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

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

            var subDirWithDot = new DirNode(DMLibTestBase.FolderName + "." + DMLibTestBase.FolderName);

            for (int i = 0; i < specialChars.Length; ++i)
            {
                string fileName = DMLibTestBase.FileName + specialChars[i] + DMLibTestBase.FileName;

                if (random.Next(2) == 0)
                {
                    if ((specialChars[i] != '.')
                        && (random.Next(2) == 0))
                    {
                        string folderName = DMLibTestBase.FolderName + specialChars[i] + DMLibTestBase.FolderName;

                        var subDir = new DirNode(folderName);
                        DMLibDataHelper.AddOneFileInBytes(subDir, fileName, 1024);
                        FileNode fileNode1 = subDir.GetFileNode(fileName);
                        fileNode1.SnapshotsCount = 1;

                        sourceDataInfo.RootNode.AddDirNode(subDir);
                    }
                    else
                    {
                        DMLibDataHelper.AddOneFileInBytes(subDirWithDot, fileName, 1024);
                        FileNode fileNode1 = subDirWithDot.GetFileNode(fileName);
                        fileNode1.SnapshotsCount = 1;
                    }
                }
                else
                {
                    DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, fileName, 1024);
                    FileNode fileNode1 = sourceDataInfo.RootNode.GetFileNode(fileName);
                    fileNode1.SnapshotsCount = 1;
                }
            }

            sourceDataInfo.RootNode.AddDirNode(subDirWithDot);

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
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void TestDirectoryFileShareSnapshots()
        {
            string snapshotFile = "snapshotFile";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            sourceDataInfo.IsFileShareSnapshot = true;
            DMLibDataHelper.AddMultipleFiles(sourceDataInfo.RootNode, snapshotFile, 10, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;

            // transfer with default options
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic dirOptions = DefaultTransferDirectoryOptions;
                dirOptions.Recursive = true;

                transferItem.Options = dirOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // verify that Files in Share Snapshot is transferred
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.CloudFileSource)]
        public void TestSingleFileShareSnapshots()
        {
            string snapshotFile = "snapshotFile";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            sourceDataInfo.IsFileShareSnapshot = true;
            DMLibDataHelper.AddMultipleFiles(sourceDataInfo.RootNode, snapshotFile, 3, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = false;

            // transfer with default options
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = DefaultTransferOptions;

                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // verify that Files in Share Snapshot is transferred
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudFile)]
        [DMLibTestMethod(DMLibDataType.CloudFile, DMLibDataType.CloudFile, isAsync: true)]
        public void TestFileShareSnapshotsCopyToBase()
        {
            int fileCount = 3;

            string snapshotFile = "snapshotFile";
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            sourceDataInfo.IsFileShareSnapshot = true;
            DMLibDataHelper.AddMultipleFiles(sourceDataInfo.RootNode, snapshotFile, fileCount, 1024);

            SourceAdaptor.Cleanup();
            SourceAdaptor.CreateIfNotExists();
            SourceAdaptor.GenerateData(sourceDataInfo);

            CloudFileDataAdaptor fileAdaptor = (SourceAdaptor as CloudFileDataAdaptor);

            CloudFileDirectory SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceDataInfo.RootNode) as CloudFileDirectory;
            CloudFileDirectory DestObject = fileAdaptor.fileHelper.FileClient.GetShareReference(fileAdaptor.ShareName).GetRootDirectoryReference();


            // transfer File and finished succssfully
            Task<TransferStatus> task = TransferManager.CopyDirectoryAsync(
                SourceObject,
                DestObject,
                DMLibTestContext.IsAsync,
                new CopyDirectoryOptions() { Recursive = true },
                new DirectoryTransferContext() { ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite });
            Test.Assert(task.Wait(15 * 60 * 100), "Tansfer finished in time.");
            Test.Assert(task.Result.NumberOfFilesFailed == 0, "No Failed File.");
            Test.Assert(task.Result.NumberOfFilesSkipped == 0, "No Skipped File.");
            Test.Assert(task.Result.NumberOfFilesTransferred == fileCount, string.Format("Transferred file :{0} == {1}", task.Result.NumberOfFilesTransferred, fileCount));

            // verify that Files in Share Snapshot is transferred
            IEnumerable<IListFileItem> sourceFiles = SourceObject.ListFilesAndDirectories();
            foreach (IListFileItem item in sourceFiles)
            {
                if (item is CloudFile)
                {
                    CloudFile srcFile = item as CloudFile;
                    CloudFile destFile = DestObject.GetFileReference(srcFile.Name);
                    srcFile.FetchAttributes();
                    destFile.FetchAttributes();
                    Test.Assert(srcFile.Properties.ContentMD5 == destFile.Properties.ContentMD5, string.Format("File {0} MD5 :{1} == {2}", srcFile.Name, srcFile.Properties.ContentMD5, destFile.Properties.ContentMD5));
                }
            }            
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.BlockBlob, DMLibDataType.CloudFile)]
        public void TestFileShareSnapshotsDest()
        {
            string failError1 = "Failed to validate destination";
            string failError2 = "Cannot perform this operation on a share representing a snapshot.";

            //Prepare Data
            string snapshotFile = "snapshotFile";
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFile(sourceDataInfo.RootNode, snapshotFile, 1024);
            SourceAdaptor.GenerateData(sourceDataInfo);

            DMLibDataInfo DestDataInfo = new DMLibDataInfo(string.Empty);
            DestDataInfo.IsFileShareSnapshot = true;
            DestAdaptor.GenerateData(DestDataInfo);

            CloudBlobDirectory SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceDataInfo.RootNode) as CloudBlobDirectory;
            CloudFileDirectory DestObject = DestAdaptor.GetTransferObject(DestDataInfo.RootPath, DestDataInfo.RootNode) as CloudFileDirectory;

            // transfer File and failed with expected Error
            Task<TransferStatus> task = TransferManager.CopyDirectoryAsync(
                SourceObject,
                DestObject,
                DMLibTestContext.IsAsync,
                new CopyDirectoryOptions() { Recursive = true },
                new DirectoryTransferContext() { ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite  });
            try
            {
                task.Wait(15 * 60 * 100);
            }
            catch (Exception e)
            {
                Test.Assert(e.InnerException.Message.Contains(failError1), "Tansfer Exception should contain:" + failError1);
                Test.Assert(e.InnerException.InnerException.Message.Contains(failError2), "Tansfer Exception should contain:" + failError2);
            }
        }

    }
}
