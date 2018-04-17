//------------------------------------------------------------------------------
// <copyright file="OverwriteTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using System;
    using System.Threading;

    [MultiDirectionTestClass]
    public class OverwriteTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

#if DNXCORE50
        public OverwriteTest()
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
            Test.Info("Class Initialize: OverwriteTest");
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
        public void OverwriteDestination()
        {
            string destExistYName = "destExistY";
            string destExistNName = "destExistN";
            string destNotExistYName = "destNotExistY";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistYName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistNName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistYName, 1024);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistYName, 1024);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistNName, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                options.DestTransferDataInfo = destDataInfo;
            }

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                string fileName = fileNode.Name;
                TransferContext transferContext = new SingleTransferContext();

                if (fileName.Equals(destExistYName))
                {
                    transferContext.ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY();
                }
                else if (fileName.Equals(destExistNName))
                {
                    transferContext.ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackN();
                }
                else if (fileName.Equals(destNotExistYName))
                {
                    transferContext.ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY();
                }

                transferItem.TransferContext = transferContext;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            DMLibDataInfo expectedDataInfo = new DMLibDataInfo(string.Empty);
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                expectedDataInfo.RootNode.AddFileNode(sourceDataInfo.RootNode.GetFileNode(destExistYName));
                expectedDataInfo.RootNode.AddFileNode(destDataInfo.RootNode.GetFileNode(destExistNName));
                expectedDataInfo.RootNode.AddFileNode(sourceDataInfo.RootNode.GetFileNode(destNotExistYName));
            }
            else
            {
                expectedDataInfo = sourceDataInfo;
            }

            // Verify transfer result
            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");

            // Verify exception
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                Test.Assert(result.Exceptions.Count == 1, "Verify there's only one exceptions.");
                TransferException transferException = result.Exceptions[0] as TransferException;
                Test.Assert(transferException != null, "Verify the exception is a TransferException");

                VerificationHelper.VerifyTransferException(transferException, TransferErrorCode.NotOverwriteExistingDestination,
                    "Skiped file", destExistNName);
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void DirectoryOverwriteDestination()
        {
            string destExistYName = "destExistY";
            string destExistNName = "destExistN";
            string destNotExistYName = "destNotExistY";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistYName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistNName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistYName, 1024);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistYName, 1024);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistNName, 1024);

            TransferContext transferContext = new DirectoryTransferContext();
            transferContext.ShouldOverwriteCallbackAsync = async (source, destination) =>
            {
                if (DMLibTestHelper.TransferInstanceToString(source).EndsWith(destExistNName))
                {
                    return false;
                }
                else
                {
                    return true;
                }
            };

            int skipCount = 0;
            int successCount = 0;
            transferContext.FileSkipped += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref skipCount);
                TransferException transferException = args.Exception as TransferException;
                Test.Assert(transferException != null, "Verify the exception is a TransferException");

                VerificationHelper.VerifyTransferException(transferException, TransferErrorCode.NotOverwriteExistingDestination,
                    "Skiped file", destExistNName);
            };

            transferContext.FileTransferred += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref successCount);
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                options.DestTransferDataInfo = destDataInfo;
            }

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = transferContext;

                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            DMLibDataInfo expectedDataInfo = new DMLibDataInfo(string.Empty);
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                expectedDataInfo.RootNode.AddFileNode(sourceDataInfo.RootNode.GetFileNode(destExistYName));
                expectedDataInfo.RootNode.AddFileNode(destDataInfo.RootNode.GetFileNode(destExistNName));
                expectedDataInfo.RootNode.AddFileNode(sourceDataInfo.RootNode.GetFileNode(destNotExistYName));
            }
            else
            {
                expectedDataInfo = sourceDataInfo;
            }

            // Verify transfer result
            Test.Assert(DMLibDataHelper.Equals(expectedDataInfo, result.DataInfo), "Verify transfer result.");

            // Verify exception
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                VerificationHelper.VerifySingleTransferStatus(result, 2, 1, 0, 1024 * 2);
                Test.Assert(successCount == 2, "Verify success transfers");
                Test.Assert(skipCount == 1, "Verify skipped transfer");
            }
            else
            {
                VerificationHelper.VerifySingleTransferStatus(result, 3, 0, 0, 1024 * 3);
                Test.Assert(successCount == 3, "Very all transfers are success");
                Test.Assert(skipCount == 0, "Very no transfer is skipped");
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.AllValidDirection)]
        public void ForceOverwriteTest()
        {
            string destExistName = "destExist";
            string destNotExistName = "destNotExist";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistName, 1024);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistName, 1024);

            TransferContext transferContext = new SingleTransferContext();
            transferContext.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;

            int skipCount = 0;
            int successCount = 0;
            transferContext.FileSkipped += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref skipCount);
            };

            transferContext.FileTransferred += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref successCount);
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                options.DestTransferDataInfo = destDataInfo;
            }

            if (IsCloudService(DMLibTestContext.DestType))
            {
                SharedAccessPermissions permissions;

                if (DMLibTestContext.IsAsync)
                {
                    permissions = SharedAccessPermissions.Write | SharedAccessPermissions.Read;
                }
                else
                {
                    permissions = SharedAccessPermissions.Write;
                }

                StorageCredentials destSAS = new StorageCredentials(DestAdaptor.GenerateSAS(permissions, (int)new TimeSpan(1, 0, 0, 0).TotalSeconds));
                options.DestCredentials = destSAS;
            }

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = transferContext;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // Verify transfer result
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            Test.Assert(successCount == 2, "Verify success transfers");
            Test.Assert(skipCount == 0, "Verify skipped transfer");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void DirectoryForceOverwriteTest()
        {
            string destExistName = "destExist";
            string destNotExistName = "destNotExist";

            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destExistName, 1024);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistName, 1024);

            DMLibDataInfo destDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(destDataInfo.RootNode, destExistName, 1024);

            TransferContext transferContext = new DirectoryTransferContext();
            transferContext.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;

            int skipCount = 0;
            int successCount = 0;
            transferContext.FileSkipped += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref skipCount);
            };

            transferContext.FileTransferred += (object sender, TransferEventArgs args) =>
            {
                Interlocked.Increment(ref successCount);
            };

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = true;
            if (DMLibTestContext.DestType != DMLibDataType.Stream)
            {
                options.DestTransferDataInfo = destDataInfo;
            }

            if (IsCloudService(DMLibTestContext.DestType))
            {
                SharedAccessPermissions permissions;

                if (DMLibTestContext.IsAsync)
                {
                    permissions = SharedAccessPermissions.Write | SharedAccessPermissions.Read;
                }
                else
                {
                    permissions = SharedAccessPermissions.Write;
                }

                StorageCredentials destSAS = new StorageCredentials(DestAdaptor.GenerateSAS(permissions, (int)new TimeSpan(1, 0, 0, 0).TotalSeconds));
                options.DestCredentials = destSAS;
            }

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                transferItem.TransferContext = transferContext;

                dynamic transferOptions = DefaultTransferDirectoryOptions;
                transferOptions.Recursive = true;
                transferItem.Options = transferOptions;
            };

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            // Verify transfer result
            Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
            VerificationHelper.VerifySingleTransferStatus(result, 2, 0, 0, 1024 * 2);
            Test.Assert(successCount == 2, "Verify success transfers");
            Test.Assert(skipCount == 0, "Verify skipped transfer");
        }
    }
}
