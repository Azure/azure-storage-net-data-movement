using System;
using System.Collections.Generic;
using System.Threading;

namespace Microsoft.Azure.Storage.DataMovement
{
    internal static class RetryExecutor
    {
        public static T ExecuteWithRetry<T>(
            Func<T> action,
            Action<Exception, int> onRetry,
            TimeSpan retryInterval,
            int maxAttemptCount)
        {
            var exceptions = new Stack<Exception>(maxAttemptCount);

            for (var attempt = 0; attempt < maxAttemptCount; attempt++)
            {
                try
                {
                    if (attempt > 0)
                    {
                        Thread.Sleep(retryInterval);
                        onRetry(exceptions.Peek(), attempt);
                    }

                    return action();
                }
                catch (Exception ex)
                {
                    exceptions.Push(ex);
                }
            }
            throw new AggregateException(exceptions);
        }
    }
}
