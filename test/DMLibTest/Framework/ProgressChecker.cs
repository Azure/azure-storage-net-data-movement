//------------------------------------------------------------------------------
// <copyright file="ProgressChecker.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
using System.Threading;
using Microsoft.WindowsAzure.Storage.DataMovement;
using MS.Test.Common.MsTestLib;

    public class ProgressChecker : IProgress<TransferProgress>
    {
        private ProgressValue<long> transferedNumber = new ProgressValue<long>();
        private ProgressValue<long> failedNumber = new ProgressValue<long>();
        private ProgressValue<long> skippedNumber = new ProgressValue<long>();
        private ProgressValue<long> transferedBytes = new ProgressValue<long>();
        private long totalNumber = 0;
        private long totalBytes = 0;
        private ManualResetEvent dataTransferred;

        public ProgressChecker(long totalNumber, long totalBytes) : this(totalNumber, totalBytes, totalNumber, 0, 0, totalBytes)
        {
        }

        public ProgressChecker(long totalNumber, long totalBytes, long transferedNumber, long failedNumber, long skippedNumber, long transferedBytes)
        {
            this.totalNumber = totalNumber;
            this.totalBytes = totalBytes;
            this.transferedNumber.MaxValue = transferedNumber;
            this.failedNumber.MaxValue = failedNumber;
            this.skippedNumber.MaxValue = skippedNumber;
            this.transferedBytes.MaxValue = transferedBytes;
            this.dataTransferred = new ManualResetEvent(false);
        }

        public IProgress<TransferProgress> GetProgressHandler()
        {
            return this;
        }

        public void Report(TransferProgress progress)
        {
            this.dataTransferred.Set();
            Test.Info("Check progress: {0}", progress.BytesTransferred);
            this.CheckIncrease(this.transferedBytes, progress.BytesTransferred, "BytesTransferred");
            this.CheckIncrease(this.transferedNumber, progress.NumberOfFilesTransferred, "NumberOfFilesTransferred");
            this.CheckIncrease(this.failedNumber, progress.NumberOfFilesFailed, "NumberOfFilesFailed");
            this.CheckIncrease(this.skippedNumber, progress.NumberOfFilesSkipped, "NumberOfFilesSkipped");
        }

        public void Reset()
        {
            this.transferedNumber.PreviousValue = 0;
            this.failedNumber.PreviousValue = 0;
            this.skippedNumber.PreviousValue = 0;
            this.transferedBytes.PreviousValue = 0;
            this.dataTransferred.Reset();
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
            if (currentValue.CompareTo(progressValue.PreviousValue) < 0 ||
                currentValue.CompareTo(progressValue.MaxValue) > 0)
            {
                Test.Error("Wrong {0} value: {1}, previous value: {2}, max value {3}", valueName, currentValue, progressValue.PreviousValue, progressValue.MaxValue);
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
        }

        public T MaxValue;
        public T PreviousValue;
    }
}
