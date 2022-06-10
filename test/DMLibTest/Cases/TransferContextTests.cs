//------------------------------------------------------------------------------
// <copyright file="ChunkedMemoryStreamTests.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MS.Test.Common.MsTestLib;
using ResX = DMLibTest.Resources;

namespace DMLibTest.Cases
{
#if DNXCORE50
    using Xunit;

    [Collection(Collections.Global)]
    public class TransferContextTests : DMLibTestBase, IClassFixture<TransferContextTestFixture>, IDisposable
    {
        public TransferContextTests()
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
    public class TransferContextTests : DMLibTestBase
    {
#endif

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void
            ShouldThrowWhenDeserializingAndDisableJournalValidationOptionIsFalseAndJournalFileHasOldVersion_DirectoryTransferContext()
        {
            // Arrange
            var stream = GetOldVersionJournal();

            // Act
            void Action()
            {
                _ = new DirectoryTransferContext(stream, false);
            }

            // Assert
#if DNXCORE50
            Assert.Throws<InvalidOperationException>(Action);
#else
            Assert.ThrowsException<InvalidOperationException>(Action,
                $"A transfer context should throw exception of type {nameof(InvalidOperationException)}, but no exception was thrown.");
#endif
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ShouldThrowWhenDeserializingAndDisableJournalValidationOptionIsFalseAndJournalFileHasOldVersion_SingleTransferContext()
        {
            // Arrange
            var stream = GetOldVersionJournal();

            // Act
            void Action()
            {
                _ = new SingleTransferContext(stream, false);
            }

            // Assert
#if DNXCORE50
            Assert.Throws<InvalidOperationException>(Action);
#else
            Assert.ThrowsException<InvalidOperationException>(Action,
                $"A transfer context should throw exception of type {nameof(InvalidOperationException)}, but no exception was thrown.");
#endif
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ShouldDeserializeWhenDisableJournalValidationOptionIsTrueAndJournalFileHasOldVersion_SingleTransferContext()
        {
            // Arrange
            var stream = GetOldVersionJournal();

            // Act
            var result = new SingleTransferContext(stream, true);

            // Assert
#if DNXCORE50
            Assert.NotNull(result);
#else
            Assert.IsNotNull(result);
#endif
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ShouldDeserializeWhenDisableJournalValidationOptionIsTrueAndJournalFileHasOldVersion_DirectoryTransferContext()
        {
            // Arrange
            var stream = GetOldVersionJournal();

            // Act
            var result = new DirectoryTransferContext(stream, true);

            // Assert
#if DNXCORE50
            Assert.NotNull(result);
#else
            Assert.IsNotNull(result);
#endif
        }

        private static Stream GetOldVersionJournal()
        {
#if DNXCORE50
            return new MemoryStream(ResX.jounal_3_1_1_0_netstandard2_0);
#else
            return new MemoryStream(ResX.jounal_3_1_1_0_net462);
#endif
        }

        #region Additional test attributes

        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: TransferContextTests");
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