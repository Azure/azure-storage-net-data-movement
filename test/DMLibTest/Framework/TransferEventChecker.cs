namespace DMLibTest
{
    using System.Threading;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public class TransferEventChecker
    {
        private int transferredFilesNum = 0;
        private int skippedFilesNum = 0;
        private int failedFilesNum = 0;

        public TransferEventChecker()
        {
        }

        public int TransferredFilesNumber
        {
            get
            {
                return this.transferredFilesNum;
            }
        }

        public int SkippedFilesNumber
        {
            get
            {
                return this.skippedFilesNum;
            }
        }

        public int FailedFilesNumber
        {
            get
            {
                return this.failedFilesNum;
            }
        }

        public void Apply(TransferContext context)
        {
            context.FileTransferred += (sender, transferEventArgs) => 
            {
                Test.Info("Transfer succeeds: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(transferEventArgs.Source), DMLibTestHelper.TransferInstanceToString(transferEventArgs.Destination));
                this.Increase(TransferEventType.Transferred); 
            };

            context.FileSkipped += (sender, transferEventArgs) => 
            {
                Test.Info("Transfer skips: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(transferEventArgs.Source), DMLibTestHelper.TransferInstanceToString(transferEventArgs.Destination));
                this.Increase(TransferEventType.Skippied); 
            };

            context.FileFailed += (sender, transferEventArgs) => 
            {
                Test.Info("Transfer fails: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(transferEventArgs.Source), DMLibTestHelper.TransferInstanceToString(transferEventArgs.Destination));
                Test.Info("Exception: {0}", transferEventArgs.Exception.ToString());
                this.Increase(TransferEventType.Failed); 
            };
        }

        public void Reset()
        {
            this.transferredFilesNum = 0;
            this.skippedFilesNum = 0;
            this.failedFilesNum = 0;
        }

        private void Increase(TransferEventType eventType)
        {
            switch (eventType)
            {
                case TransferEventType.Transferred:
                    Interlocked.Increment(ref this.transferredFilesNum);
                    break;
                case TransferEventType.Skippied:
                    Interlocked.Increment(ref this.skippedFilesNum);
                    break;
                case TransferEventType.Failed:
                    Interlocked.Increment(ref this.failedFilesNum);
                    break;
                default:
                    break;
            }
        }
    }

    enum TransferEventType
    {
        Transferred,
        Skippied,
        Failed
    }
}
