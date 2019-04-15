//------------------------------------------------------------------------------
// <copyright file="ProgressChecker.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Threading;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public class ProgressChecker : IProgress<TransferStatus>
    {
        private ProgressValue<long> transferedNumber;
        private ProgressValue<long> failedNumber;
        private ProgressValue<long> skippedNumber;
        private ProgressValue<long> transferedBytes;
        private long totalNumber = 0;
        private long totalBytes = 0;
        private ManualResetEvent dataTransferred;

        public ProgressChecker() : this(0, 0, null, null, null, null)
        {
        }

        public ProgressChecker(long totalNumber, long totalBytes) : this(totalNumber, totalBytes, null, null, null, null)
        {
        }

        public ProgressChecker(long totalNumber, long totalBytes, long? transferedNumber, long? failedNumber, long? skippedNumber, long? transferedBytes)
        {
            this.totalNumber = totalNumber;
            this.totalBytes = totalBytes;

            this.transferedNumber = this.CreateProgressValue(transferedNumber);
            this.failedNumber = this.CreateProgressValue(failedNumber);
            this.skippedNumber = this.CreateProgressValue(skippedNumber);
            this.transferedBytes = this.CreateProgressValue(transferedBytes);

            this.dataTransferred = new ManualResetEvent(false);
        }

        public long TransferredFilesNumber
        {
            get
            {
                return this.transferedNumber.PreviousValue;
            }
        }

        public long SkippedFilesNumber
        {
            get
            {
                return this.skippedNumber.PreviousValue;
            }
        }

        public long FailedFilesNumber
        {
            get
            {
                return this.failedNumber.PreviousValue;
            }
        }

        private ProgressValue<long> CreateProgressValue(long? maxValue)
        {
            
            return new ProgressValue<long>()
            {
                MaxValue = maxValue??default(long),
                IgnoreCheck = maxValue == null,
            };
        }

        public IProgress<TransferStatus> GetProgressHandler()
        {
            return this;
        }

        public void Report(TransferStatus progress)
        {
            if (progress.BytesTransferred > 0)
            {
                this.dataTransferred.Set();
            }

            Test.Info("Check progress: {0}", progress.BytesTransferred);
            this.CheckIncrease(this.transferedBytes, progress.BytesTransferred, "BytesTransferred");
            this.CheckIncrease(this.transferedNumber, progress.NumberOfFilesTransferred, "NumberOfFilesTransferred");
            this.CheckIncrease(this.failedNumber, progress.NumberOfFilesFailed, "NumberOfFilesFailed");
            this.CheckIncrease(this.skippedNumber, progress.NumberOfFilesSkipped, "NumberOfFilesSkipped");
        }

        public void Reset()
        {
            this.Reset(this.transferedNumber);
            this.Reset(this.skippedNumber);
            this.Reset(this.failedNumber);
            this.Reset(this.transferedBytes);
            this.dataTransferred.Reset();
        }

        private void Reset<T>(ProgressValue<T> progressValue) where T : IComparable<T>
        {
            progressValue.PreviousValue = default(T);
        }

        public WaitHandle DataTransferred
        {
            get
            {
                return this.dataTransferred;
            }
        }

        private void CheckEqual<T>(T expectedValue, T currentValue, string valueName) where T : IComparable<T>
        {
            if (currentValue.CompareTo(expectedValue) != 0)
            {
                Test.Error("Wrong {0} value: {1}, expected value: {2}", valueName, currentValue, expectedValue);
            }
        }

        private void CheckIncrease<T>(ProgressValue<T> progressValue, T currentValue, string valueName) where T : IComparable<T>
        {
            // ignore check
            if (!progressValue.IgnoreCheck)
            {
                if (currentValue.CompareTo(progressValue.PreviousValue) < 0 ||
                    currentValue.CompareTo(progressValue.MaxValue) > 0)
                {
                    Test.Error("Wrong {0} value: {1}, previous value: {2}, max value {3}", valueName, currentValue, progressValue.PreviousValue, progressValue.MaxValue);
                }
            }

            progressValue.PreviousValue = currentValue;
        }
    }

    class ProgressValue<T> where T : IComparable<T>
    {
        public ProgressValue()
        {
            this.MaxValue = default(T);
            this.PreviousValue = default(T);
            this.IgnoreCheck = false;
        }

        public bool IgnoreCheck;
        public T MaxValue;
        public T PreviousValue;
    }
}
