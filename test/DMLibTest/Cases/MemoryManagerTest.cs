namespace DMLibTest.Cases
{
	using System;
	using Microsoft.Azure.Storage.DataMovement;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using MS.Test.Common.MsTestLib;

#if DNXCORE50
    using Xunit;
	using Assert = Xunit.Assert;

    [Collection(Collections.Global)]
    public class MemoryManagerTest : DMLibTestBase, IClassFixture<EnumerationTasksLimitManagerTestFixture>, IDisposable
    {

		public MemoryManagerTest()
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
	public class MemoryManagerTest : DMLibTestBase
	{
#endif
		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldNotCreateMoreBuffersThanMemoryLimitationAllows()
		{
			// Arrange
			var sut = new MemoryManager(10, 1);
			sut.SetMemoryLimitation(10);

			var clientRequestId = Guid.NewGuid().ToString();
			sut.RequireBuffers(clientRequestId, 10);

			// Act
			byte[] bufferAboveTheLimit = sut.RequireBuffer(clientRequestId);

			// Assert
			// when null, no new buffer was allowed to be created
#if DNXCORE50
			Assert.Equal(null, bufferAboveTheLimit);
#else
			Assert.IsNull(bufferAboveTheLimit);
#endif
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldAllowToCreateNewBufferWhenMemoryManagerWasFullAndAllBuffersWereReleased()
		{
			// Arrange
			var sut = new MemoryManager(10, 1);
			sut.SetMemoryLimitation(10);

			var clientRequestId = Guid.NewGuid().ToString();
			sut.RequireBuffers(clientRequestId, 10);

			// Act
			sut.ReleaseBuffers(clientRequestId);

			// Assert
			// The only way to check this is to try require a new buffer
#if DNXCORE50
			Assert.NotEqual(null, sut.RequireBuffer(clientRequestId));
#else
			Assert.IsNotNull(sut.RequireBuffer(clientRequestId));
#endif
		}

		[TestMethod]
		[TestCategory(Tag.Function)]
#if DNXCORE50
		[TestStartEnd()]
#endif
		public void ShouldNotRemoveBuffersOfOtherClientRequestIdWhenReleasingAllBuffersById()
		{
			// Arrange
			var sut = new MemoryManager(10, 1);
			sut.SetMemoryLimitation(10);
			var clientRequestId1 = Guid.NewGuid().ToString();
			sut.RequireBuffers(clientRequestId1, 5);
			var clientRequestId2 = Guid.NewGuid().ToString();
			sut.RequireBuffers(clientRequestId2, 5);

			// Act
			// After initial buffer saturation in Arrange, test releases memory of clientRequestId1, but clientRequestId2 should remain intact.
			sut.ReleaseBuffers(clientRequestId1);

			// It verifies that after clientRequestId2 can allocate more buffers,  and...
			sut.RequireBuffers(clientRequestId2, 5);

			// Assert
			//... there's no space left for clientRequestId1 buffers.
			byte[] bufferAboveTheLimit = sut.RequireBuffer(clientRequestId1);
#if DNXCORE50
			Assert.Equal(null, bufferAboveTheLimit);
#else
			Assert.IsNull(bufferAboveTheLimit);
#endif
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