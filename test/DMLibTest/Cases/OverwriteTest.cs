//------------------------------------------------------------------------------
// <copyright file="OverwriteTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest.Cases
{
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class OverwriteTest : DMLibTestBase
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
                TransferContext transferContext = new TransferContext();

                if (fileName.Equals(destExistYName))
                {
                    transferContext.OverwriteCallback = DMLibInputHelper.GetDefaultOverwiteCallbackY();
                }
                else if (fileName.Equals(destExistNName))
                {
                    transferContext.OverwriteCallback = DMLibInputHelper.GetDefaultOverwiteCallbackN();
                }
                else if (fileName.Equals(destNotExistYName))
                {
                    transferContext.OverwriteCallback = DMLibInputHelper.GetDefaultOverwiteCallbackY();
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
    }
}
