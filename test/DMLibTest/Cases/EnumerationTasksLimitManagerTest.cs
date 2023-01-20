namespace DMLibTest.Cases
{
	using System;
	using System.Threading;
	using Microsoft.Azure.Storage.DataMovement;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Moq;
	using MS.Test.Common.MsTestLib;

#if DNXCORE50
    using Xunit;
	using Assert = Xunit.Assert;

    [Collection(Collections.Global)]
    public class EnumerationTasksLimitManagerTest : DMLibTestBase, IClassFixture<EnumerationTasksLimitManagerTestFixture>, IDisposable
    {

		public EnumerationTasksLimitManagerTest()
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
#else
	[TestClass]
	public class EnumerationTasksLimitManagerTest : DMLibTestBase
	{
#endif

		const int _MAX_TRANSFER_CONCURRENCY = 10;

		private static MemoryManager memoryManager = new MemoryManager(10, _MAX_TRANSFER_CONCURRENCY);

#if !DOTNET5_4
		[DataTestMethod]
		[DataRow(1)]
		[DataRow(_MAX_TRANSFER_CONCURRENCY - 1)]
		[DataRow(_MAX_TRANSFER_CONCURRENCY)]
#endif
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[Theory]
		[TestStartEnd()]
		[InlineData(1)]
		[InlineData(_MAX_TRANSFER_CONCURRENCY - 1)]
		[InlineData(_MAX_TRANSFER_CONCURRENCY)]
#endif
		public void ShouldNotPauseWhenTasksNumberIsBelowOrEqualTheLimit(int outstandingTasksNumber)
		{
			// Arrange
			var resetEventMock = new Mock<WaitHandle>();

			var sut = new EnumerationTasksLimitManager(_MAX_TRANSFER_CONCURRENCY, resetEventMock.Object, TimeSpan.MinValue, null);

			// Act
			sut.CheckAndPauseEnumeration(outstandingTasksNumber, memoryManager, CancellationToken.None);

			// Assert
			resetEventMock.Verify(re => re.WaitOne(It.IsAny<TimeSpan>()), Times.Never);
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldThrowExceptionWhenItWaitsLongerThanTimeout()
		{
			// Arrange
			var resetEventMock = new Mock<WaitHandle>();
			var sut = new EnumerationTasksLimitManager(_MAX_TRANSFER_CONCURRENCY, resetEventMock.Object, TimeSpan.Zero, TimeSpan.FromMilliseconds(1));

			// Act
			Action action = () => sut.CheckAndPauseEnumeration(_MAX_TRANSFER_CONCURRENCY + 1, memoryManager, CancellationToken.None);

			// Assert
#if DNXCORE50
			Assert.Throws<TransferStuckException>(action);
#else
			Assert.ThrowsException<TransferStuckException>(action,
				$"Delegate should throw exception of type {nameof(TransferStuckException)}, but no exception was thrown.");
#endif
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldStopWaitingWhenHandleWasSignaled()
		{
			// Arrange
			var resetEventMock = new Mock<WaitHandle>();
			resetEventMock.SetupSequence(re => re.WaitOne(It.IsAny<TimeSpan>())).Returns(false).Returns(false).Returns(true);

			var sut = new EnumerationTasksLimitManager(_MAX_TRANSFER_CONCURRENCY, resetEventMock.Object, TimeSpan.Zero, TimeSpan.FromSeconds(10));

			// Act
			sut.CheckAndPauseEnumeration(_MAX_TRANSFER_CONCURRENCY + 1, memoryManager, CancellationToken.None);

			// Assert
			resetEventMock.Verify();
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldStopWaitingWhenCancelled()
		{
			// Arrange
			var resetEventMock = new Mock<WaitHandle>();
			var cts = new CancellationTokenSource();

			var cycles = 0;
			resetEventMock.Setup(re => re.WaitOne(It.IsAny<TimeSpan>())).Callback(() =>
			{
				cycles++;
				if (cycles > 3)
				{
					cts.Cancel();
				}
			});

			var sut = new EnumerationTasksLimitManager(_MAX_TRANSFER_CONCURRENCY, resetEventMock.Object, TimeSpan.Zero, TimeSpan.FromSeconds(10));

			// Act
			sut.CheckAndPauseEnumeration(_MAX_TRANSFER_CONCURRENCY + 1, memoryManager, cts.Token);

			// Assert
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldNotThrowWhenStuckTimeoutIsNotSet()
		{
			// Arrange
			var resetEventMock = new Mock<WaitHandle>();
			resetEventMock.SetupSequence(re => re.WaitOne(It.IsAny<TimeSpan>())).Returns(false).Returns(true);
			var sut = new EnumerationTasksLimitManager(_MAX_TRANSFER_CONCURRENCY, resetEventMock.Object, TimeSpan.Zero, null);

			// Act
			sut.CheckAndPauseEnumeration(_MAX_TRANSFER_CONCURRENCY + 1, memoryManager, CancellationToken.None);

			// Assert
			resetEventMock.Verify();
		}


		#region Additional test attributes

		[ClassInitialize]
		public static void MyClassInitialize(TestContext testContext)
		{
			Test.Info("Class Initialize: EnumerationTasksLimitManagerTest");
			BaseClassInitialize(testContext);
			CleanupSource = false;
		}

		[ClassCleanup]
		public static void MyClassCleanup()
		{
			BaseClassCleanup();
		}

		[TestInitialize]
		public void MyTestInitialize()
		{
			base.BaseTestInitialize();
		}

		[TestCleanup]
		public void MyTestCleanup()
		{
			base.BaseTestCleanup();

		}

		#endregion
	}
}
