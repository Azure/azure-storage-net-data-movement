//------------------------------------------------------------------------------
// <copyright file="CheckContentMD5Test.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class CheckContentMD5Test : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public CheckContentMD5Test()
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
            Test.Info("Class Initialize: CheckContentMD5Test");
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
            TransferContext context = new DirectoryTransferContext();
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
            List<Exception> transferExceptions = new List<Exception>();
            context.FileFailed += (eventSource, eventArgs) =>
            {
                transferExceptions.Add(eventArgs.Exception);
            };

            TransferItem checkMD5Item = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
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
                CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
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

            if (testResult.Exceptions.Count != 0 || transferExceptions.Count != 1)
            {
                Test.Error("Expect one exception but actually no exception is thrown.");
            }
            else
            {
                VerificationHelper.VerifyExceptionErrorMessage(transferExceptions[0], new string[] { "The MD5 hash calculated from the downloaded data does not match the MD5 hash stored in the property of source" });
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void TestDirectoryCheckContentMD5Resume()
        {
            long fileSize = 5 * 1024;
            int fileCountMulti = 32;
            long totalSize = fileSize * 4 * fileCountMulti;
            string wrongMD5 = "wrongMD5";

            string checkWrongMD5File = "checkWrongMD5File";
            string checkCorrectMD5File = "checkCorrectMD5File";
            string notCheckWrongMD5File = "notCheckWrongMD5File";
            string notCheckCorrectMD5File = "notCheckCorrectMD5File";

            // Prepare data for transfer items with checkMD5
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DirNode checkMD5Folder = new DirNode("checkMD5");
            for (int i = 0; i < fileCountMulti; ++i)
            {
                var wrongMD5FileNode = new FileNode($"{checkWrongMD5File}_{i}")
                {
                    SizeInByte = fileSize,
                    MD5 = wrongMD5
                };

                checkMD5Folder.AddFileNode(wrongMD5FileNode);

                DMLibDataHelper.AddOneFileInBytes(checkMD5Folder, $"{checkCorrectMD5File}_{i}", fileSize);
            }
            sourceDataInfo.RootNode.AddDirNode(checkMD5Folder);

            // Prepare data for transfer items with disabling MD5 check
            DirNode notCheckMD5Folder = new DirNode("notCheckMD5");

            for (int i = 0; i < fileCountMulti; ++i)
            {
                var wrongMD5FileNode = new FileNode($"{notCheckWrongMD5File}_{i}")
                {
                    SizeInByte = fileSize,
                    MD5 = wrongMD5
                };

                notCheckMD5Folder.AddFileNode(wrongMD5FileNode);

                DMLibDataHelper.AddOneFileInBytes(notCheckMD5Folder, $"{notCheckCorrectMD5File}_{i}", fileSize);
            }

            sourceDataInfo.RootNode.AddDirNode(notCheckMD5Folder);
            
            SourceAdaptor.GenerateData(sourceDataInfo);

            TransferEventChecker eventChecker = new TransferEventChecker();
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            TransferContext context = new DirectoryTransferContext();
            eventChecker.Apply(context);
            
            ProgressChecker progressChecker = new ProgressChecker(4 * fileCountMulti, totalSize, 3 * fileCountMulti, null, 0, totalSize);
            context.ProgressHandler = progressChecker.GetProgressHandler();
            List<Exception> transferExceptions = new List<Exception>();
           
            TransferItem checkMD5Item = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                TransferContext = context,
                Options = new DownloadDirectoryOptions()
                {
                    DisableContentMD5Validation = false,
                    Recursive = true,
                },
                CancellationToken = cancellationTokenSource.Token,
            };

            TransferItem notCheckMD5Item = new TransferItem()
            {
                SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, notCheckMD5Folder),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, notCheckMD5Folder),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                TransferContext = context,
                Options = new DownloadDirectoryOptions()
                {
                    DisableContentMD5Validation = true,
                    Recursive = true,
                },
                CancellationToken = cancellationTokenSource.Token
            };

            var executionOption = new TestExecutionOptions<DMLibDataInfo>();
            executionOption.AfterAllItemAdded = () =>
            {
                // Wait until there are data transferred
                if (!progressChecker.DataTransferred.WaitOne(30000))
                {
                    Test.Error("No progress in 30s.");
                }

                // Cancel the transfer and store the second checkpoint
                cancellationTokenSource.Cancel();
            };
            executionOption.LimitSpeed = true;

            var testResult = this.RunTransferItems(new List<TransferItem>() { checkMD5Item, notCheckMD5Item }, executionOption);

            eventChecker = new TransferEventChecker();
            context = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(context.LastCheckpoint));
            eventChecker.Apply(context);

            bool failureReported = false;
            context.FileFailed += (sender, args) =>
            {
                if (args.Exception != null)
                {
                    failureReported = args.Exception.Message.Contains(checkWrongMD5File);
                }

                transferExceptions.Add(args.Exception);
            };

            progressChecker.Reset();
            context.ProgressHandler = progressChecker.GetProgressHandler();

            checkMD5Item = checkMD5Item.Clone();
            notCheckMD5Item = notCheckMD5Item.Clone();

            checkMD5Item.TransferContext = context;
            notCheckMD5Item.TransferContext = context;

            testResult = this.RunTransferItems(new List<TransferItem>() { checkMD5Item, notCheckMD5Item }, new TestExecutionOptions<DMLibDataInfo>());

            DMLibDataInfo expectedDataInfo = sourceDataInfo.Clone();
            DMLibDataInfo actualDataInfo = testResult.DataInfo;
            for (int i = 0; i < fileCountMulti; ++i)
            {
                expectedDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode($"{checkWrongMD5File}_{i}");
                expectedDataInfo.RootNode.GetDirNode(notCheckMD5Folder.Name).DeleteFileNode($"{notCheckWrongMD5File}_{i}");
                actualDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode($"{checkWrongMD5File}_{i}");
                actualDataInfo.RootNode.GetDirNode(notCheckMD5Folder.Name).DeleteFileNode($"{notCheckWrongMD5File}_{i}");
            }

            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, actualDataInfo), "Verify transfer result.");
            Test.Assert(failureReported, "Verify md5 check failure is reported.");
            VerificationHelper.VerifyFinalProgress(progressChecker, 3 * fileCountMulti, 0, fileCountMulti);

            if (testResult.Exceptions.Count != 0 || transferExceptions.Count != fileCountMulti)
            {
                Test.Error("Expect one exception but actually no exception is thrown.");
            }
            else
            {
                for (int i = 0; i < fileCountMulti; ++i)
                {
                    VerificationHelper.VerifyExceptionErrorMessage(transferExceptions[i], new string[] { "The MD5 hash calculated from the downloaded data does not match the MD5 hash stored in the property of source" });
                }
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.LocalDest)]
        public void TestDirectoryCheckContentMD5StreamResume()
        {
            TestDirectoryCheckContentMD5StreamResume(true);
            TestDirectoryCheckContentMD5StreamResume(false);
        }

        private void TestDirectoryCheckContentMD5StreamResume(bool checkMD5)
        {
            long fileSize = 5 * 1024;
            int fileCountMulti = 32;
            long totalSize = fileSize * 4 * fileCountMulti;
            string wrongMD5 = "wrongMD5";

            string wrongMD5File = "wrongMD5File";
            string correctMD5File = "correctMD5File";

            // Prepare data for transfer items with checkMD5
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DirNode checkMD5Folder = new DirNode(checkMD5 ? "checkMD5" : "notCheckMD5");
            for (int i = 0; i < fileCountMulti; ++i)
            {
                var wrongMD5FileNode = new FileNode($"{wrongMD5File}_{i}")
                {
                    SizeInByte = fileSize,
                    MD5 = wrongMD5
                };

                checkMD5Folder.AddFileNode(wrongMD5FileNode);

                DMLibDataHelper.AddOneFileInBytes(checkMD5Folder, $"{correctMD5File}_{i}", fileSize);
            }
            sourceDataInfo.RootNode.AddDirNode(checkMD5Folder);

            SourceAdaptor.GenerateData(sourceDataInfo);
            DestAdaptor.Cleanup();

            TransferEventChecker eventChecker = new TransferEventChecker();
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            using (var resumeStream = new MemoryStream())
            {
                TransferContext context = new DirectoryTransferContext(resumeStream);
                eventChecker.Apply(context);

                ProgressChecker progressChecker = new ProgressChecker(2 * fileCountMulti,
                    totalSize, checkMD5 ? fileCountMulti : 2 * fileCountMulti, null, 0, totalSize);

                context.ProgressHandler = progressChecker.GetProgressHandler();
                List<Exception> transferExceptions = new List<Exception>();

                TransferItem checkMD5Item = new TransferItem()
                {
                    SourceObject = SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                    DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, checkMD5Folder),
                    IsDirectoryTransfer = true,
                    SourceType = DMLibTestContext.SourceType,
                    DestType = DMLibTestContext.DestType,
                    CopyMethod = DMLibTestContext.CopyMethod.ToCopyMethod(),
                    TransferContext = context,
                    Options = new DownloadDirectoryOptions()
                    {
                        DisableContentMD5Validation = !checkMD5,
                        Recursive = true,
                    },
                    CancellationToken = cancellationTokenSource.Token,
                };

                var executionOption = new TestExecutionOptions<DMLibDataInfo>();
                executionOption.AfterAllItemAdded = () =>
                {
                    // Wait until there are data transferred
                    if (!progressChecker.DataTransferred.WaitOne(30000))
                    {
                        Test.Error("No progress in 30s.");
                    }

                    // Cancel the transfer and store the second checkpoint
                    cancellationTokenSource.Cancel();
                };
                executionOption.LimitSpeed = true;

                var testResult = this.RunTransferItems(new List<TransferItem>() { checkMD5Item }, executionOption);

                if (null != testResult.Exceptions)
                {
                    foreach (var exception in testResult.Exceptions)
                    {
                        Test.Info("Got exception during transferring. {0}", exception);
                    }
                }

                eventChecker = new TransferEventChecker();
                resumeStream.Position = 0;
                context = new DirectoryTransferContext(resumeStream);
                eventChecker.Apply(context);

                bool failureReported = false;
                context.FileFailed += (sender, args) =>
                {
                    if (args.Exception != null)
                    {
                        failureReported = args.Exception.Message.Contains(wrongMD5File);
                    }

                    transferExceptions.Add(args.Exception);
                };

                progressChecker.Reset();
                context.ProgressHandler = progressChecker.GetProgressHandler();

                checkMD5Item = checkMD5Item.Clone();

                checkMD5Item.TransferContext = context;

                testResult = this.RunTransferItems(new List<TransferItem>() { checkMD5Item }, new TestExecutionOptions<DMLibDataInfo>());

                DMLibDataInfo expectedDataInfo = sourceDataInfo.Clone();
                DMLibDataInfo actualDataInfo = testResult.DataInfo;
                for (int i = 0; i < fileCountMulti; ++i)
                {
                    expectedDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode($"{wrongMD5File}_{i}");
                    actualDataInfo.RootNode.GetDirNode(checkMD5Folder.Name).DeleteFileNode($"{wrongMD5File}_{i}");
                }

                Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, actualDataInfo), "Verify transfer result.");
                Test.Assert(checkMD5 ? failureReported : !failureReported, "Verify md5 check failure is expected.");
                VerificationHelper.VerifyFinalProgress(progressChecker, checkMD5 ? fileCountMulti : 2 * fileCountMulti, 0, checkMD5 ? fileCountMulti : 0);

                if (checkMD5)
                {
                    if (testResult.Exceptions.Count != 0 || transferExceptions.Count != fileCountMulti)
                    {
                        Test.Error("Expect one exception but actually no exception is thrown.");
                    }
                    else
                    {
                        for (int i = 0; i < fileCountMulti; ++i)
                        {
                            VerificationHelper.VerifyExceptionErrorMessage(transferExceptions[i], new string[] { "The MD5 hash calculated from the downloaded data does not match the MD5 hash stored in the property of source" });
                        }
                    }
                }
                else
                {
                    Test.Assert(testResult.Exceptions.Count == 0, "Should no exception thrown out when disabling check md5");
                }
            }
        }
    }
}
