using System.Threading.Tasks;

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Threading;

    internal class RetryableMD5HashStream : MD5HashStream
    {
        private readonly int retryCount;

        private readonly TimeSpan retryInterval;

        public RetryableMD5HashStream(Stream stream,
            long lastTransferOffset,
            bool md5HashCheck,
            IDataMovementLogger logger,
            TimeSpan retryBaseInterval, 
            int retryCount) : base(stream, lastTransferOffset, md5HashCheck, logger)
        {
            retryInterval = retryBaseInterval;
            this.retryCount = retryCount;
        }

        public override void CalculateMd5(MemoryManager memoryManager, Action checkCancellation)
        {
            CalculateMd5(memoryManager, checkCancellation, retryCount);
        }

        public override Task<int> ReadAsync(long readOffset, byte[][] buffers, int offset, int count,
            CancellationToken cancellationToken)
        {
            return ReadAsync(readOffset, buffers, offset, count, cancellationToken, retryCount);
        }

        private void CalculateMd5(MemoryManager memoryManager, Action checkCancellation, int retryCount)
        {
            try
            {
                base.CalculateMd5(memoryManager, checkCancellation);
            }
            catch (IOException ioException) when (ioException.Message.Contains("An unexpected network error occurred."))
            {
                // In case of intermittent network issues with a network drive, retry the operation
                if (retryCount > 0)
                {
                    Thread.Sleep(GetInterval(retryCount));
                    this.CalculateMd5(memoryManager, checkCancellation, retryCount - 1);
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task<int> ReadAsync(long readOffset, byte[][] buffers, int offset, int count, CancellationToken cancellationToken, int retryCount)
        {
            try
            {
                return await base.ReadAsync(readOffset, buffers, offset, count, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException ioException) when (ioException.Message.Contains("An unexpected network error occurred."))
            {
                // In case of intermittent network issues with a network drive, retry the operation
                if (retryCount > 0)
                {
                    Thread.Sleep(GetInterval(retryCount));
                    return await this.ReadAsync(readOffset, buffers, offset, count, cancellationToken, retryCount - 1);
                }
                else
                {
                    throw;
                }
            }
        }

        private TimeSpan GetInterval(int retryNumber)
        {
            return TimeSpan.FromSeconds(Math.Pow(retryInterval.TotalSeconds, retryCount - retryNumber));
        }
    }
}
