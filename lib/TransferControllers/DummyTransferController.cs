//------------------------------------------------------------------------------
// <copyright file="DummyTransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;


    /// <summary>
    /// Transfer controller for dummy objects.
    /// Creates dummy objects only, no data transfer will happen.
    /// </summary>
    internal class DummyTransferController : TransferControllerBase
    {
        private enum Status
        {
            NotStarted,
            Started,
            Finished,
            ErrorOccured
        };
        private Status status = Status.NotStarted;

        public DummyTransferController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
            if (null == transferScheduler)
            {
                throw new ArgumentNullException(nameof(transferScheduler));
            }

            if (null == transferJob)
            {
                throw new ArgumentNullException(nameof(transferJob));
            }

            if (null == transferJob.CheckPoint)
            {
                transferJob.CheckPoint = new SingleObjectCheckpoint();
            }
        }

        public override bool HasWork
        {
            get
            {
                return status == Status.NotStarted;
            }
        }

        protected override async Task<bool> DoWorkInternalAsync()
        {
            status = Status.Started;

            await Task.Run(
                () =>
                {
                    if (this.TransferJob.Source.Type == TransferLocationType.AzureBlob
                        && this.TransferJob.Destination.Type == TransferLocationType.FilePath)
                    {
                        // Dummy transfer for downloading dummy blobs.
                        var filePath = (this.TransferJob.Destination as FileLocation).FilePath;
                        if (LongPathFile.Exists(filePath))
                        {
                            string exceptionMessage = string.Format(
                                        CultureInfo.CurrentCulture,
                                        Resources.FailedToCreateDirectoryException,
                                        filePath);

                            throw new TransferException(
                                    TransferErrorCode.FailedToCreateDirectory,
                                    exceptionMessage);
                        }
                        else
                        {
                            LongPathDirectory.CreateDirectory(filePath);
                        }
                    }
                    // Hint: adding new dummy directions here.
                    else
                    {
                        string exceptionMessage = string.Format(
                                    CultureInfo.CurrentCulture,
                                    Resources.UnsupportedDummyTransferException);

                        throw new TransferException(
                                TransferErrorCode.UnsupportedDummyTransfer,
                                exceptionMessage);
                    }

                    status = Status.Finished;
                }, this.CancellationToken);
            return status == Status.Finished || status == Status.ErrorOccured;
        }

        protected override void SetErrorState(Exception ex)
        {
            status = Status.ErrorOccured;
        }
    }
}
