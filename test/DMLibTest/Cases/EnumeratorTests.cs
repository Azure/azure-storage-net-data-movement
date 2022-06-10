//------------------------------------------------------------------------------
// <copyright file="ChunkedMemoryStreamTests.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MS.Test.Common.MsTestLib;

namespace DMLibTest.Cases
{
#if DNXCORE50
    using Xunit;

    [Collection(Collections.Global)]
    public class EnumeratorTests : DMLibTestBase, IClassFixture<EnumeratorTestFixture>, IDisposable
    {
        public EnumeratorTests()
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
    public class EnumeratorTests : DMLibTestBase
    {
#endif
        private const string DirectoryName1 = DirName + "1";
        private const string DirectoryName2 = DirName + "2";
        private string dataSetPath;

#if !DOTNET5_4
        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ShouldNotThrowWhenSubDirectoryIsDeletedDuringEnumeration()
        {
            // Act
            var ex = GetExceptionForAction(EnumerateDataSet);

            // Assert
#if DNXCORE50
            Assert.Null(ex);
#else
            Assert.IsNull(ex);
#endif
        }
#endif

        private void EnumerateDataSet()
        {
            var items = EnumerateDirectoryHelper.EnumerateInDirectory(dataSetPath, "*", string.Empty,
                SearchOption.AllDirectories, false, CancellationToken.None);

            // Subdirectories are enumerated after files.
            // When item from a sub-directory is met, the second directory can be removed (no parallelism, entire enumeration is sequential).
            // The second directory still will be in a queue to enumerate. This force code to invoke SymlinkedDirExists method (Linux only) which will throw exception.
            foreach (var item in items) DeleteSubDirectoryIfConditionsAreMet(item.Path);
        }

#if DOTNET5_4
        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ShouldThrowOnLinuxWhenSubDirectoryIsDeletedDuringEnumeration()
        {
            // Arrange
            if (!CrossPlatformHelpers.IsLinux)
            {
                // Exception can be thrown only in Linux. All other platforms ignore a not existing directory
                Assert.True(true);
                return;
            }

            // Act
            Action action = EnumerateDataSet;

            // Assert

#if DNXCORE50
            Assert.Throws<DirectoryNotFoundException>(action);
#else
            Assert.ThrowsException<DirectoryNotFoundException>(action,
                $"Delegate should throw exception of type {nameof(DirectoryNotFoundException)}, but no exception was thrown.");
#endif
        }
#endif

        private void DeleteSubDirectoryIfConditionsAreMet(string path)
        {
            if (path.EndsWith(Path.Combine(DirectoryName1, FileName)))
                Directory.Delete(Path.Combine(dataSetPath, DirectoryName2), true);

            if (path.EndsWith(Path.Combine(DirectoryName2, FileName)))
                Directory.Delete(Path.Combine(dataSetPath, DirectoryName1), true);
        }

        private void BuildDataSet()
        {
            Directory.CreateDirectory(dataSetPath);
            var filePath = Path.Combine(dataSetPath, FileName);
            File.WriteAllText(filePath, string.Empty);
            var dir1Path = Path.Combine(dataSetPath, DirectoryName1);
            Directory.CreateDirectory(dir1Path);
            File.Copy(filePath, Path.Combine(dir1Path, FileName));
            var dir2Path = Path.Combine(dataSetPath, DirectoryName2);
            Directory.CreateDirectory(dir2Path);
            File.Copy(filePath, Path.Combine(dir2Path, FileName));
        }

        private static Exception GetExceptionForAction(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                return ex;
            }

            return null;
        }

#region Additional test attributes

        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: EnumeratorTests");
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
            dataSetPath = Path.Combine(Path.GetTempPath(), "DMLib", Guid.NewGuid().ToString());
            BuildDataSet();
        }

        [TestCleanup]
        public void MyTestCleanup()
        {
            base.BaseTestCleanup();

            try
            {
                if (Directory.Exists(dataSetPath)) Directory.Delete(dataSetPath, true);
            }
            catch (Exception)
            {
                // ignored
            }
        }

#endregion

    }
}
