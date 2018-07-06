//------------------------------------------------------------------------------
// <copyright file="SASTokenVersionTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest.Cases
{
    using System;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class SASTokenVersionTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        #region Initialization and cleanup methods

        static readonly string[] SASVersions =
            {
                "2012-02-12",
                "2013-08-15",
                "2014-02-14",
                "2015-02-21",
                "2015-04-05",
                "2015-07-08",
                "2015-12-11"
            };

#if DNXCORE50
        public SASTokenVersionTest()
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
            Test.Info("Class Initialize: SASTokenVersionTest");
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
        public void TestSingleTransferSASTokenOfEachVersion()
        {
            foreach (var targetSASVersion in SASVersions)
            {
                this.TestSASTokenOfEachVersion(targetSASVersion, false);
            }
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirAllValidDirection)]
        public void TestDirTransferSASTokenOfEachVersion()
        {
            foreach (var targetSASVersion in SASVersions)
            {
                this.TestSASTokenOfEachVersion(targetSASVersion, true);
            }
        }

        private void TestSASTokenOfEachVersion(string targetSASVersion, bool isDirectoryTransfer)
        {
            Test.Info("Testing version of {0}", targetSASVersion);
            DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
            DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, DMLibTestBase.FileName, 1024);

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.IsDirectoryTransfer = isDirectoryTransfer;

            options.TransferItemModifier = (fileNode, transferItem) =>
            {
                dynamic transferOptions = isDirectoryTransfer ? DefaultTransferDirectoryOptions : DefaultTransferOptions;

                transferItem.Options = transferOptions;

                if (isDirectoryTransfer)
                {
                    transferItem.TransferContext = new DirectoryTransferContext();

                    transferItem.TransferContext.FileFailed += (source, e) =>
                       {
                           Test.Error(e.Exception.ToString());
                       };

                    DirectoryOptions dirOptions = transferItem.Options as DirectoryOptions;
                    dirOptions.Recursive = true;
                }
                else
                {
                    transferItem.TransferContext = new SingleTransferContext();
                }

                transferItem.TransferContext.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;
            };
            

            string sourceSAS = null;
            string destSAS = null;

            switch (DMLibTestContext.SourceType)
            {
                case DMLibDataType.CloudBlob:
                case DMLibDataType.AppendBlob:
                case DMLibDataType.BlockBlob:
                case DMLibDataType.PageBlob:
                    if ((DMLibTestContext.SourceType == DMLibDataType.AppendBlob)
                        && (string.CompareOrdinal(targetSASVersion, "2015-04-05") < 0))
                    {
                        break;
                    }

                    SourceAdaptor.CreateIfNotExists();
                    CloudBlobDataAdaptor blobAdaptor = SourceAdaptor as CloudBlobDataAdaptor;
                    sourceSAS = Util.SASGenerator.GetSharedAccessSignature(blobAdaptor.GetBaseContainer(),
                        new SharedAccessBlobPolicy
                        {
                            Permissions = isDirectoryTransfer ? SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.List : SharedAccessBlobPermissions.Read,
                            SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(1)
                        },
                        null,
                        null,
                        null,
                        targetSASVersion);
                    break;
                case DMLibDataType.CloudFile:
                    if (string.CompareOrdinal(targetSASVersion, "2015-02-21") < 0)
                    {
                        break;
                    }

                    SourceAdaptor.CreateIfNotExists();
                    CloudFileDataAdaptor fileAdaptor = SourceAdaptor as CloudFileDataAdaptor;
                    sourceSAS = Util.SASGenerator.GetSharedAccessSignature(
                        fileAdaptor.GetBaseShare(),
                        new SharedAccessFilePolicy
                        {
                            Permissions = isDirectoryTransfer ? SharedAccessFilePermissions.List | SharedAccessFilePermissions.Read : SharedAccessFilePermissions.Read,
                            SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(1)
                        },
                        null,
                        null,
                        null,
                        targetSASVersion);
                    break;
                default:
                    break;
            }

            if (!DMLibTestContext.IsAsync
                || (string.CompareOrdinal(targetSASVersion, "2014-02-14") >= 0))
            {
                switch (DMLibTestContext.DestType)
                {
                    case DMLibDataType.CloudBlob:
                    case DMLibDataType.AppendBlob:
                    case DMLibDataType.BlockBlob:
                    case DMLibDataType.PageBlob:
                        if ((DMLibTestContext.DestType == DMLibDataType.AppendBlob)
                            && (string.CompareOrdinal(targetSASVersion, "2015-04-05") < 0))
                        {
                            break;
                        }

                        DestAdaptor.CreateIfNotExists();
                        CloudBlobDataAdaptor blobAdaptor = DestAdaptor as CloudBlobDataAdaptor;
                        destSAS = Util.SASGenerator.GetSharedAccessSignature(blobAdaptor.GetBaseContainer(),
                            new SharedAccessBlobPolicy
                            {
                                Permissions = DMLibTestContext.IsAsync ? SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read : SharedAccessBlobPermissions.Write,
                                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(1)
                            },
                            null,
                            null,
                            null,
                            targetSASVersion);
                        break;
                    case DMLibDataType.CloudFile:
                        if (string.CompareOrdinal(targetSASVersion, "2015-02-21") < 0)
                        {
                            break;
                        }

                        DestAdaptor.CreateIfNotExists();
                        CloudFileDataAdaptor fileAdaptor = DestAdaptor as CloudFileDataAdaptor;
                        destSAS = Util.SASGenerator.GetSharedAccessSignature(
                            fileAdaptor.GetBaseShare(),
                            new SharedAccessFilePolicy
                            {
                                Permissions = DMLibTestContext.IsAsync ? SharedAccessFilePermissions.Write | SharedAccessFilePermissions.Read : SharedAccessFilePermissions.Write,
                                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(1)
                            },
                            null,
                            null,
                            null,
                            targetSASVersion);
                        break;
                    default:
                        break;
                }
            }

            if (null != sourceSAS)
            {
                options.SourceCredentials = new StorageCredentials(sourceSAS);
            }

            if (null != destSAS)
            {
                options.DestCredentials = new StorageCredentials(destSAS);
            }

            var result = this.ExecuteTestCase(sourceDataInfo, options);

            VerificationHelper.VerifyTransferSucceed(result, sourceDataInfo);
        }
    }
}
