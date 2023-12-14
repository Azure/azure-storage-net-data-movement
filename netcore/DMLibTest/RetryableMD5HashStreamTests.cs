using System;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace RetryableMD5HashStreamTests
{
    using Xunit;
    using Microsoft.Azure.Storage.DataMovement;
    using System.IO;

    public class RetryableMD5HashStreamTests
    {
        [Fact]
        public void ShouldRetryMd5CalculationOnNetworkError()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            var networkError = new IOException("Test: An unexpected network error occurred.");
            mockStream.Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>()).Throws(networkError);
            mockStream.CanSeek.Returns(true);
            mockStream.CanRead.Returns(true);

            var mockMemoryManager = new MemoryManager(1, 100);
            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, 1, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act
            Assert.Throws<IOException>(() => sut.CalculateMd5(mockMemoryManager, () => { }));

            // Assert
            mockStream.Received(4).Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>());
        }

        [Fact]
        public void ShouldNotRetryMd5CalculationWhenNoNetworkError()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            var error = new Exception("Test: An unexpected error occurred.");
            mockStream.Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>()).Throws(error);
            mockStream.CanSeek.Returns(true);
            mockStream.CanRead.Returns(true);

            var mockMemoryManager = new MemoryManager(1, 100);
            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, 1, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act 
            Assert.Throws<Exception>(() => sut.CalculateMd5(mockMemoryManager, () => { }));

            // Assert
            mockStream.Received(1).Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>());
        }

        [Fact]
        public void ShouldNotRetryMd5CalculationWhenNoErrors()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            const int streamLength = 1;
            mockStream.Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>()).Returns(streamLength);
            mockStream.CanSeek.Returns(true);
            mockStream.CanRead.Returns(true);

            var mockMemoryManager = new MemoryManager(1, 100);
            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, streamLength, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act 
            sut.CalculateMd5(mockMemoryManager, () => { });

            // Assert
            mockStream.Received(1).Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>());
        }

        [Fact]
        public async Task ShouldRetryReadAsyncOnNetworkError()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            var networkError = new IOException("Test: An unexpected network error occurred.");
            mockStream.ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Throws(networkError);
            mockStream.CanSeek.Returns(true);
            mockStream.CanRead.Returns(true);

            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, 1, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act
            await Assert.ThrowsAsync<IOException>(() => sut.ReadAsync(0, new byte[1][], 0, 0, CancellationToken.None));

            // Assert
            await mockStream.Received(4).ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).ConfigureAwait(false);
        }

        [Fact]
        public async Task ShouldNotRetryReadAsyncWhenNoNetworkError()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            var error = new Exception("Test: An unexpected error occurred.");
            mockStream.ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Throws(error);
            mockStream.CanSeek.Returns(true);
            mockStream.CanRead.Returns(true);

            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, 1, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act
            await Assert.ThrowsAsync<Exception>(() => sut.ReadAsync(0, new byte[1][], 0, 0, CancellationToken.None)).ConfigureAwait(false);

            // Assert
            await mockStream.Received(1).ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).ConfigureAwait(false);
        }

        [Fact]
        public async Task ShouldNotRetryReadAsyncWhenNoErrors()
        {
            // Arrange
            var mockStream = Substitute.For<Stream>();
            var mockLogger = Substitute.For<IDataMovementLogger>();

            var sut = new RetryableMD5HashStream(mockStream, 0, true, mockLogger, TimeSpan.FromSeconds(0), 3);

            // Act
            await sut.ReadAsync(0, new byte[1][], 0, 0, CancellationToken.None).ConfigureAwait(false);

            // Assert
            await mockStream.Received(1).ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).ConfigureAwait(false);
        }
    }
}
