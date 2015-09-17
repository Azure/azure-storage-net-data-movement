//------------------------------------------------------------------------------
// <copyright file="CheckContentMD5Test.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using System;
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
    }
}
