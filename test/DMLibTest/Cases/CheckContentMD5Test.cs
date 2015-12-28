//------------------------------------------------------------------------------
// <copyright file="CheckContentMD5Test.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using System;
    using System.Collections.Generic;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class CheckContentMD5Test : DMLibTestBase
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
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void TestCheckContentMD5()
        {
            long fileSize = 10 * 1024 * 1024;
            string wrongMD5 = "wrongMD5";

            string checkWrongMD5File = "checkWrongMD5File";
            string notCheckWrongMD5File = "notCheckWrongMD5File";
            string checkCorrectMD5File = "checkCorrectMD5File";
            string notCheckCorrectMD5File = "notCheckCorrectMD5File";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, checkWrongMD5File, fileSize);
            FileNode tmpFileNode = sourceDataInfo.RootNode.GetFileNode(checkWrongMD5File);
            tmpFileNode.MD5 = wrongMD5;

            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, notCheckWrongMD5File, fileSize);
            tmpFileNode = sourceDataInfo.RootNode.GetFileNode(notCheckWrongMD5File);
            tmpFileNode.MD5 = wrongMD5;

            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, checkCorrectMD5File, fileSize);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, notCheckCorrectMD5File, fileSize);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                string fileName = fileNode.Name;
                DownloadOptions downloadOptions = new DownloadOptions();
                if (fileName.Equals(checkWrongMD5File) || fileName.Equals(checkCorrectMD5File))
                {
                    downloadOptions.DisableContentMD5Validation = false;
                }
                else if (fileName.Equals(notCheckWrongMD5File) || fileName.Equals(notCheckCorrectMD5File))
                {
                    downloadOptions.DisableContentMD5Validation = true;
                }

                transferItem.Options = downloadOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            Test.Assert(result.Exceptions.Count == 1, "Verify there's one exception.");
            Exception exception = result.Exceptions[0];

            Test.Assert(exception is InvalidOperationException, "Verify it's an invalid operation exception.");
            VerificationHelper.VerifyExceptionErrorMessage(exception, "The MD5 hash calculated from the downloaded data does not match the MD5 hash stored", checkWrongMD5File);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void TestDirectoryCheckContentMD5()
        {
            long fileSize = 5 * 1024 * 1024;
            long totalSize = fileSize * 4;
            string wrongMD5 = "wrongMD5";

            string checkWrongMD5File = "checkWrongMD5File";
            string checkCorrectMD5File = "checkCorrectMD5File";
            string notCheckWrongMD5File = "notCheckWrongMD5File";
            string notCheckCorrectMD5File = "notCheckCorrectMD5File";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DirNode checkMD5Folder = new DirNode("checkMD5");
            DMLibDataHelper.AddOneFileInBytes(checkMD5Folder, checkWrongMD5File, fileSize);
            DMLibDataHelper.AddOneFileInBytes(checkMD5Folder, checkCorrectMD5File, fileSize);
            sourceDataInfo.RootNode.AddDirNode(checkMD5Folder);

            DirNode notCheckMD5Folder = new DirNode("notCheckMD5");
            DMLibDataHelper.AddOneFileInBytes(notCheckMD5Folder, notCheckWrongMD5File, fileSize);
            DMLibDataHelper.AddOneFileInBytes(notCheckMD5Folder, notCheckCorrectMD5File, fileSize);
            sourceDataInfo.RootNode.AddDirNode(notCheckMD5Folder);

            FileNode tmpFileNode = checkMD5Folder.GetFileNode(checkWrongMD5File);
            tmpFileNode.MD5 = wrongMD5;

            tmpFileNode = notCheckMD5Folder.GetFileNode(notCheckWrongMD5File);
            tmpFileNode.MD5 = wrongMD5;

            SourceAdaptor.GenerateData(sourceDataInfo);

            TransferEventChecker eventChecker = new TransferEventChecker();
            TransferContext context = new TransferContext();
            eventChecker.Apply(context);
            
            bool failureReported = false;
            context.FileFailed += (sender, args) =>
                {
                    if (args.Exception != null)
                    {
                        failureReported = args.Exception.Message.Contains(checkWrongMD5File);
                    }
                };

            ProgressChecker progressChecker = new ProgressChecker(4, totalSize, 3, 1, 0, totalSize);
            context.ProgressHandler = progressChecker.GetProgressHandler();

            TransferItem checkMD5Item = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                IsServiceCopy = DMLibTestContext.IsAsync,
                TransferContext = context,
                Options = new DownloadDirectoryOptions()
                {
                    DisableContentMD5Validation = false,
                    Recursive = true,
                },
            };

            TransferItem notCheckMD5Item = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, notCheckMD5Folder),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, notCheckMD5Folder),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                IsServiceCopy = DMLibTestContext.IsAsync,
                TransferContext = context,
                Options = new DownloadDirectoryOptions()
                {
                    DisableContentMD5Validation = true,
                    Recursive = true,
                },
            };

            var testResult = this.RunTransferItems(new List<TransferItem>() { checkMD5Item, notCheckMD5Item }, new TestExecutionOptions<DMLibDataInfo>());

            DMLibDataInfo expectedDataInfo = sourceDataInfo.Clone();
            expectedDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode(checkWrongMD5File);
            expectedDataInfo.RootNode.GetDirNode(notCheckMD5Folder.Name).DeleteFileNode(notCheckWrongMD5File);

            DMLibDataInfo actualDataInfo = testResult.DataInfo;
            actualDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode(checkWrongMD5File);
            actualDataInfo.RootNode.GetDirNode(notCheckMD5Folder.Name).DeleteFileNode(notCheckWrongMD5File);

            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, actualDataInfo), "Verify transfer result.");
            Test.Assert(failureReported, "Verify md5 check failure is reported.");
            VerificationHelper.VerifyFinalProgress(progressChecker, 3, 0, 1);

            if (testResult.Exceptions.Count != 1)
            {
                Test.Error("Expect one exception but actually no exception is thrown.");
            }
            else
            {
                VerificationHelper.VerifyTransferException(testResult.Exceptions[0], TransferErrorCode.SubTransferFails, "1 sub transfer(s) failed.");
            }
        }
    }
}
