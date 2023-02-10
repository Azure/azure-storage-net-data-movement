using System;
using System.Threading;
using Timer = System.Timers.Timer;

namespace Microsoft.Azure.Storage.DataMovement.Client.Progress
{
    internal class TimedProgressStatus : IProgress<TransferStatus>
    {
        private readonly string jobId;
        private TransferStatus lastStatus;

        public TimedProgressStatus(string jobId)
        {
            this.jobId = jobId;
            var timer = new Timer(15000);
            timer.Elapsed += (_, _) => LogStatistics();
            timer.Start();
        }

        public void Report(TransferStatus status)
        {
            Interlocked.Exchange(ref lastStatus, status);
        }

        private void LogStatistics()
        {
            if (lastStatus == null) return;

            Console.WriteLine(
                $"({jobId}) Bytes transferred: {lastStatus.BytesTransferred} B, Files transferred: {lastStatus.NumberOfFilesTransferred}, Files failed: {lastStatus.NumberOfFilesFailed}, Files skipped: {lastStatus.NumberOfFilesSkipped}");
        }
    }
}