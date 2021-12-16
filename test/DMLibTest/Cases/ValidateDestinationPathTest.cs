namespace DMLibTest.Cases
{
	using DMLibTestCodeGen;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Microsoft.Azure.Storage.Auth;
	using Microsoft.Azure.Storage.DataMovement;
	using MS.Test.Common.MsTestLib;
	using System;
	using System.Threading;
	using System.Threading.Tasks;

	[MultiDirectionTestClass] 
	public class ValidateDestinationPathTest : DMLibTestBase
#if DNXCORE50
        , IDisposable
#endif
	{

		#region Initialization and cleanup methods

#if DNXCORE50
        public ValidateDestinationPathTest()
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
			Test.Info("Class Initialize: ValidateSourcePathTest");
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

		// TODO Due to file access exception those tests are ignored. DMLib starts a few threads and all of them try to open the same file

		[TestCategory(Tag.Function)]
		[DMLibTestMethodSet(DMLibTestMethodSet.AllSync, Ignore = true)]
		public void ShouldTransferWhenDestinationPathValidationSucceeded()
		{
			string destNotExistYName = "destNotExistY";

			DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
			DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistYName, 1024);

			int skipCount = 0;
			int successCount = 0;

			TransferContext transferContext = new SingleTransferContext
			{
				ValidateDestinationPathCallbackAsync = DMLibInputHelper.GetDefaultSucceededPathValidationCallback(),
			};

			transferContext.FileSkipped += (object sender, TransferEventArgs args) => { Interlocked.Increment(ref skipCount); };
			transferContext.FileTransferred += (object sender, TransferEventArgs args) => { Interlocked.Increment(ref successCount); };

			var options = new TestExecutionOptions<DMLibDataInfo>
			{
				TransferItemModifier = (fileNode, transferItem) => { transferItem.TransferContext = transferContext; },
				IsDirectoryTransfer = false,
			};

			var result = this.ExecuteTestCase(sourceDataInfo, options);

			// Verify transfer result
			Test.Assert(DMLibDataHelper.Equals(sourceDataInfo, result.DataInfo), "Verify transfer result.");
			Test.Assert(successCount == 1, "Verify success transfers");
			Test.Assert(skipCount == 0, "Verify skipped transfer");
		}

		[TestCategory(Tag.Function)]
		[DMLibTestMethodSet(DMLibTestMethodSet.AllSync, Ignore = true)]
		public void ShouldFailWhenDestinationPathValidationFailed()
		{
			string destNotExistYName = "destNotExistY";

			DMLibDataInfo sourceDataInfo = new DMLibDataInfo(string.Empty);
			DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, destNotExistYName, 1024);

			int skipCount = 0;
			int successCount = 0;

			TransferContext transferContext = new SingleTransferContext
			{
				ValidateDestinationPathCallbackAsync = DMLibInputHelper.GetDefaultFailedPathValidationCallback(),
			};

			var options = new TestExecutionOptions<DMLibDataInfo>
			{
				TransferItemModifier = (fileNode, transferItem) => { transferItem.TransferContext = transferContext; },
				IsDirectoryTransfer = false,
			};

			var result = this.ExecuteTestCase(sourceDataInfo, options);

			// Verify exception
			if (DMLibTestContext.DestType != DMLibDataType.Stream)
			{
				Test.Assert(result.Exceptions.Count == 1, "Verify there's only one exceptions.");
				TransferException transferException = result.Exceptions[0] as TransferException;
				Test.Assert(transferException != null, "Verify the exception is a TransferException");

				VerificationHelper.VerifyTransferException(transferException, TransferErrorCode.PathCustomValidationFailed,
					"Skipped file", destNotExistYName);
			}
		}
	}
}