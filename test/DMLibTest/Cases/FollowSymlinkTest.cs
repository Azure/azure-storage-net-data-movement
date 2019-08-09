

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class FollowSymlinkTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {

        private static string UnicodeFileName = FileOp.NextString(random, random.Next(6, 10));
        private const string FolderSuffix = "_Folder";
        private const string FileSuffix = "_File";
        private const string SymlinkSuffix = "_Link";

        #region Initialization and cleanup methods

#if DNXCORE50
        public FollowSymlinkTest()
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
            Test.Info("Class Initialize: FollowSymlinkTest");
            DMLibTestBase.BaseClassInitialize(testContext);
            
            Test.Info("Use file name {0} in BVT.", UnicodeFileName);
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
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_Set_True()
        {
#if DNXCORE50
           var uploadOption = new UploadDirectoryOptions();
            bool gotException = false;

            try
            {
                uploadOption.FollowSymlink = true;
            }
            catch (PlatformNotSupportedException)
            {
                gotException = true;
            }

            if (!CrossPlatformHelpers.IsLinux)
            {
                Test.Assert(gotException, "Should throw not supported exception on non-Linux platform.");
            }
            else
            {
                Test.Assert(uploadOption.FollowSymlink == true, "Follow symlink is set correctly");
            }
#endif
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_1_BrokenSymlink()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            var dirNode = new DirNode($"{UnicodeFileName}{FolderSuffix}");
            dirNode.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
            {
                SizeInByte = 1024
            });

            sourceDataInfo.RootNode.AddDirNode(dirNode);
            sourceDataInfo.RootNode.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}", dirNode.Name, dirNode));
            dirNode = new DirNode($"{UnicodeFileName}{FolderSuffix}_1");
            sourceDataInfo.RootNode.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}_1", dirNode.Name, dirNode));

            SourceAdaptor.GenerateData(sourceDataInfo);

            sourceDataInfo.RootNode.DeleteDirNode($"{UnicodeFileName}{SymlinkSuffix}_1");

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                DisableSourceGenerator = true,
                DisableSourceCleaner = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                },
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // For sync copy, recalculate md5 of destination by downloading the file to local.
            if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
            {
                DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
            }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
#endif
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_1_SymlinkDir()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            var dirNode = new DirNode($"{UnicodeFileName}{FolderSuffix}");
            dirNode.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
            {
                SizeInByte = 1024
            });

            sourceDataInfo.RootNode.AddDirNode(dirNode);
            sourceDataInfo.RootNode.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}", dirNode.Name, dirNode));

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                },
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // For sync copy, recalculate md5 of destination by downloading the file to local.
            if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
            {
                DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
            }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
#endif
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_Random_SymlinkDir_OnSameLevel()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            int level = random.Next(2, 40);

            for (int i = 0; i < level; ++i)
            {
                var dirNode = new DirNode($"{UnicodeFileName}{FolderSuffix}_{i}");
                dirNode.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
                {
                    SizeInByte = 1024
                });

                sourceDataInfo.RootNode.AddDirNode(dirNode);
                sourceDataInfo.RootNode.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}_{i}", dirNode.Name, dirNode));
            }

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                },
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // For sync copy, recalculate md5 of destination by downloading the file to local.
            if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
            {
                DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
            }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
#endif
        }

        /// n < 40
        /// Symlink_(n-1) -> Folder_n
        ///           -  File_(n-2)
        ///           -  Symlink_(n-2) -> Folder_(n-1)... ... Symlink_0 -> Folder_1
        ///                                                   - File_0
        ///
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_Random_Levels_SymlinkDir()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            int level = random.Next(2, 40);

            Test.Info($"Testing {level} levels of symlinked dirs");

            BuildDeepSymlinkDir(sourceDataInfo.RootNode, sourceDataInfo.RootNode, level);

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                },
            };

            // This case may takes very long time.
            options.TimeoutInMs = 60 * 60 * 1000;

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // For sync copy, recalculate md5 of destination by downloading the file to local.
            if (IsCloudService(DMLibTestContext.DestType) && !DMLibTestContext.IsAsync)
            {
                DMLibDataHelper.SetCalculatedFileMD5(result.DataInfo, DestAdaptor);
            }

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
#endif
        }

        /// n >= 40
        /// Symlink_(n-1) -> Folder_n
        ///           -  File_(n-2)
        ///           -  Symlink_(n-2) -> Folder_(n-1)... ... Symlink_0 -> Folder_1
        ///                                                   - File_0
        /// 
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_Random_Levels_40_SymlinkDir()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");

            int level = random.Next(40, 60);

            Test.Info($"Testing {level} levels of symlinked dirs");

            BuildDeepSymlinkDir(sourceDataInfo.RootNode, sourceDataInfo.RootNode, level);

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                }
            };

            // This case may takes very long time.
            options.TimeoutInMs = 60 * 60 * 1000;

            var result = this.ExecuteTestCase(sourceDataInfo, options);
            Test.Assert(result.Exceptions.Count == 1 && result.Exceptions[0] is TransferException && result.Exceptions[0].InnerException.Message.Contains("Too many levels of symbolic links"), "Verify expected exception is thrown.");
#endif
        }
        
        /// 
        /// targetDir
        ///      - symlink -> ../targetDir
        ///      - file
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_DeadLoop_1()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");
            DirNode targetFolder = new DirNode($"{UnicodeFileName}{FolderSuffix}");
            targetFolder.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
            {
                SizeInByte = 1024
            });

            targetFolder.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}", Path.Combine("..", $"{UnicodeFileName}{FolderSuffix}"), targetFolder));

            sourceDataInfo.RootNode.AddDirNode(targetFolder);

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                }
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);
            Test.Assert(result.Exceptions.Count == 1 && result.Exceptions[0] is TransferException && result.Exceptions[0].Message.Contains("Potential dead loop in directory structure due to symbolic link"), "Verify expected exception is thrown.");
#endif
        }
        
        /// Root dir struct        | target dir struct
        /// root -                 | C
        ///      - symlink-> B     |  - B
        ///                        |  - symlink -> ../../C
        ///                        |  
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FollowSymlink_DeadLoop_2()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("rootfolder");
            DMLibDataInfo targetDataInfo = new DMLibDataInfo("targetC");

            DirNode dirNodeC = new DirNode("folder_C");
            DirNode dirNodeB = new DirNode("folder_B");
            dirNodeB.AddFileNode(new FileNode("File_B")
            {
                SizeInByte = 1024
            });

            dirNodeC.AddDirNode(dirNodeB);
            dirNodeB.AddDirNode(DirNode.SymlinkedDir("symlinkToC", "../../folder_C", dirNodeC));
            targetDataInfo.RootNode.AddDirNode(dirNodeC);

            SourceAdaptor.CreateIfNotExists();
            SourceAdaptor.GenerateData(targetDataInfo);

            sourceDataInfo.RootNode.AddDirNode(DirNode.SymlinkedDir("symlinkToB", "../targetC/folder_C/folder_B", dirNodeB));

            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = true;
                    (transferOptions as UploadDirectoryOptions).FollowSymlink = true;
                    item.Options = transferOptions;
                },
                DisableSourceCleaner = true,
            };

            // This case may takes very long time.
            options.TimeoutInMs = 60 * 60 * 1000;

            var result = this.ExecuteTestCase(sourceDataInfo, options);
            foreach (var exception in result.Exceptions)
            {
                Test.Info(exception.Message);
            }

            Test.Assert(result.Exceptions.Count == 1 && result.Exceptions[0] is TransferException && result.Exceptions[0].InnerException.Message.Contains("Too many levels of symbolic links"), "Verify expected exception is thrown.");
#endif
        }
        
        /// Uupload a symlinked dir
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void Upload_Symlinked_RootDir()
        {
#if DNXCORE50
            if (!CrossPlatformHelpers.IsLinux)
            {
                return;
            }
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo("");
            DMLibDataInfo targetDataInfo = new DMLibDataInfo("target");
            UnicodeFileName = "TempTestName";

            DirNode dirNode = new DirNode($"{UnicodeFileName}{FolderSuffix}");
            dirNode.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
            {
                SizeInByte = 1024
            });
            
            targetDataInfo.RootNode.AddDirNode(dirNode);

            SourceAdaptor.CreateIfNotExists();
            DestAdaptor.CreateIfNotExists();

            SourceAdaptor.GenerateData(targetDataInfo);
            sourceDataInfo.RootNode = DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}", "target", targetDataInfo.RootNode);
            SourceAdaptor.GenerateData(sourceDataInfo);

            TransferItem item = new TransferItem()
            {
                SourceObject = Path.Combine(SourceAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceDataInfo.RootNode) as string, sourceDataInfo.RootNode.Name),
                DestObject = DestAdaptor.GetTransferObject(sourceDataInfo.RootPath, sourceDataInfo.RootNode),
                IsDirectoryTransfer = true,
                SourceType = DMLibTestContext.SourceType,
                DestType = DMLibTestContext.DestType,
                IsServiceCopy = DMLibTestContext.IsAsync,
                TransferContext = new DirectoryTransferContext(),
                Options = DefaultTransferDirectoryOptions
            };

            (item.Options as UploadDirectoryOptions).Recursive = true;

            var result = this.RunTransferItems(new List<TransferItem>() { item }, new TestExecutionOptions<DMLibDataInfo>());

            Test.Assert(result.Exceptions.Count == 0, "Verify no exception is thrown.");
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
#endif
        }

        private void BuildDeepSymlinkDir(DirNode rootDir, DirNode parent, int level)
        {
            DirNode subDir = new DirNode($"{UnicodeFileName}{FolderSuffix}_{level}");

            if (--level > 0)
            {
                BuildDeepSymlinkDir(rootDir, subDir, level);
            }

            subDir.AddFileNode(new FileNode($"{UnicodeFileName}{FileSuffix}")
            {
                SizeInByte = 1024
            });
            rootDir.AddDirNode(subDir);
            parent.AddDirNode(DirNode.SymlinkedDir($"{UnicodeFileName}{SymlinkSuffix}_{level}", Path.Combine(Object.ReferenceEquals(rootDir, parent) ? "" : "..", subDir.Name), subDir));
        }
    }
}
