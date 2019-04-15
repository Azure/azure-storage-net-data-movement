//------------------------------------------------------------------------------
// <copyright file="ChunkedMemoryStreamTests.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

#if DEBUG
namespace DMLibTest.Cases
{
    using System.IO;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Azure.Storage.DataMovement;

    [TestClass]
    public class ChunkedMemoryStreamTests
    {
        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStreamFlagsAreCorrect()
        {
            var buffers = new[] {new byte[1], new byte[1]};
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 2);

            Assert.IsTrue(chunkedMemoryStream.CanRead);
            Assert.IsTrue(chunkedMemoryStream.CanSeek);
            Assert.IsTrue(chunkedMemoryStream.CanWrite);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStreamLengthShouldBeRight()
        {
            var buffers = new[] { new byte[10], new byte[20] };
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            Assert.AreEqual(30, chunkedMemoryStream.Length);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStreamPositionAndLengthShouldBeRight()
        {
            var buffers = new[] { new byte[10], new byte[20] };
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 5, 20);

            Assert.AreEqual(20, chunkedMemoryStream.Length);
            Assert.AreEqual(0, chunkedMemoryStream.Position);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStreamCanSeekCorrectly()
        {
            var buffers = new[] { new byte[10], new byte[20] };
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            chunkedMemoryStream.Seek(10, SeekOrigin.Current);

            Assert.AreEqual(30, chunkedMemoryStream.Length);
            Assert.AreEqual(10, chunkedMemoryStream.Position);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStreamCanSetPositionCorrectly()
        {
            var buffers = new[] { new byte[10], new byte[20] };
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            chunkedMemoryStream.Position = 20;

            Assert.AreEqual(30, chunkedMemoryStream.Length);
            Assert.AreEqual(20, chunkedMemoryStream.Position);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Read_CanReadAllTheDataOutCorrectly()
        {
            var buffers = new[]
            {
                new byte[10] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                new byte[20] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29}
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);
            var outBuffer = new byte[30];

            // Read all the data
            var bytesRead = chunkedMemoryStream.Read(outBuffer, 0, 30);
            Assert.AreEqual(30, bytesRead);

            for (int i = 0; i < buffers.Length; i++)
            {
                Assert.AreEqual(i, outBuffer[i]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Read_CanReadPartOfDataOutCorrectly()
        {
            var buffers = new[]
            {
                new byte[10] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                new byte[20] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29}
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);
            var outBuffer = new byte[30];

            // Read first 20 bytes out
            var bytesRead = chunkedMemoryStream.Read(outBuffer, 0, 20);
            Assert.AreEqual(20, bytesRead);

            for (int i = 0; i < buffers.Length; i++)
            {
                Assert.AreEqual(i, outBuffer[i]);
            }

            // Read last 20 bytes out
            chunkedMemoryStream.Position = 10;
            bytesRead = chunkedMemoryStream.Read(outBuffer, 0, 20);
            Assert.AreEqual(20, bytesRead);

            for (int i = 0; i < buffers.Length; i++)
            {
                Assert.AreEqual(i + 10, outBuffer[i]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Read_CanReadAllDataOutCorrectlyWhenUndelyingBufferSizeIsGreaterThanActualLength()
        {
            var buffers = new[]
            {
                new byte[10] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                new byte[20] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29}
            };

            // Length 20 is less than the underlying buffer length 30
            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 20);
            Assert.AreEqual(20, chunkedMemoryStream.Length);

            var outBuffer = new byte[20];

            // Read all out
            var bytesRead = chunkedMemoryStream.Read(outBuffer, 0, 20);
            Assert.AreEqual(20, bytesRead);

            for (int i = 0; i < buffers.Length; i++)
            {
                Assert.AreEqual(i, outBuffer[i]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Write_CanWriteDataToFullInOneTime()
        {
            var buffers = new[]
            {
                new byte[10],
                new byte[20]
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            // Write all the data in 1 time
            var inBuffer = new byte[30];
            for (int i = 0; i < 30; i++)
            {
                inBuffer[i] = (byte)i;
            }
            chunkedMemoryStream.Write(inBuffer, 0, 30);


            Assert.AreEqual(30, chunkedMemoryStream.Position);
            Assert.AreEqual(30, chunkedMemoryStream.Length);

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer[i], buffers[0][i]);
            }

            for (int i = 0; i < 20; i++)
            {
                Assert.AreEqual(inBuffer[10+i], buffers[1][i]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Write_CanWriteDataPartiallyInOneTime()
        {
            var buffers = new[]
            {
                new byte[10],
                new byte[20]
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            // Write the data partially in 1 time
            var inBuffer = new byte[20];
            for (int i = 0; i < 20; i++)
            {
                inBuffer[i] = (byte)i;
            }
            chunkedMemoryStream.Write(inBuffer, 0, 20);


            Assert.AreEqual(20, chunkedMemoryStream.Position);
            Assert.AreEqual(30, chunkedMemoryStream.Length);

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer[i], buffers[0][i]);
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer[10 + i], buffers[1][i]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Write_CanWriteDataToFullInMultipleTimes()
        {
            var buffers = new[]
            {
                new byte[10],
                new byte[20]
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            // Write the data partially in 2 times
            var inBuffer = new byte[10];
            for (int i = 0; i < 10; i++)
            {
                inBuffer[i] = (byte)i;
            }
            chunkedMemoryStream.Write(inBuffer, 0, 10);


            var inBuffer2 = new byte[10];
            for (int i = 0; i < 10; i++)
            {
                inBuffer2[i] = (byte)(i + 10);
            }
            chunkedMemoryStream.Write(inBuffer2, 0, 10);


            var inBuffer3 = new byte[10];
            for (int i = 0; i < 10; i++)
            {
                inBuffer3[i] = (byte)(i + 20);
            }
            chunkedMemoryStream.Write(inBuffer3, 0, 10);


            Assert.AreEqual(30, chunkedMemoryStream.Position);
            Assert.AreEqual(30, chunkedMemoryStream.Length);

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer[i], buffers[0][i]);
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer2[i], buffers[1][i]);
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer3[i], buffers[1][i+10]);
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void ChunkedMemoryStream_Write_CanWriteDataPartiallyInMultipleTimes()
        {
            var buffers = new[]
            {
                new byte[10],
                new byte[20]
            };

            var chunkedMemoryStream = new ChunkedMemoryStream(buffers, 0, 30);

            // Write the data partially in 2 times
            var inBuffer = new byte[10];
            for (int i = 0; i < 10; i++)
            {
                inBuffer[i] = (byte)i;
            }
            chunkedMemoryStream.Write(inBuffer, 0, 10);


            var inBuffer2 = new byte[10];
            for (int i = 0; i < 10; i++)
            {
                inBuffer2[i] = (byte)(i + 10);
            }
            chunkedMemoryStream.Write(inBuffer2, 0, 10);


            Assert.AreEqual(20, chunkedMemoryStream.Position);
            Assert.AreEqual(30, chunkedMemoryStream.Length);

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer[i], buffers[0][i]);
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(inBuffer2[i], buffers[1][i]);
            }
        }
    }
}

#endif
