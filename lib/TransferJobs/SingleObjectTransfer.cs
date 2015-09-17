//------------------------------------------------------------------------------
// <copyright file="SingleObjectTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a single object transfer operation.
    /// </summary>
    [Serializable]
    internal class SingleObjectTransfer : Transfer
    {
        private const string TransferJobName = "TransferJob";

        /// <summary>
        /// Internal transfer jobs.
        /// </summary>
        private TransferJob transferJob;

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleObjectTransfer"/> class.
        /// This constructor will check whether source and destination is valid for the operation:
        /// Uri is only valid for non-staging copy. 
        /// cannot copy from local file/stream to local file/stream 
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public SingleObjectTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
            if (null == source)
            {
                throw new ArgumentNullException("source");
            }

            if (null == dest)
            {
                throw new ArgumentNullException("dest");
            }

            if ((null != source.FilePath || null != source.Stream)
                && (null != dest.FilePath || null != dest.Stream))
            {
                throw new InvalidOperationException(Resources.LocalToLocalTransferUnsupportedException);
            }

            if ((null != source.Blob)
                && (null != dest.Blob))
            {
                if (source.Blob.BlobType != dest.Blob.BlobType)
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationBlobTypeDifferent);
                }

                if (StorageExtensions.Equals(source.Blob, dest.Blob))
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
                }
            }

            if ((null != source.AzureFile)
                && (null != dest.AzureFile)
                && string.Equals(source.AzureFile.Uri.Host, dest.AzureFile.Uri.Host, StringComparison.OrdinalIgnoreCase)
                && string.Equals(source.AzureFile.Uri.AbsolutePath, dest.AzureFile.Uri.AbsolutePath, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
            }

            this.transferJob = new TransferJob(this.Source, this.Destination);
            this.transferJob.Transfer = this;
        }

        protected SingleObjectTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.transferJob = (TransferJob)info.GetValue(TransferJobName, typeof(TransferJob));
            this.transferJob.Transfer = this;
        }

        private SingleObjectTransfer(SingleObjectTransfer other)
            : base(other)
        {
            this.transferJob = other.transferJob.Copy();
            this.transferJob.Transfer = this;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(TransferJobName, this.transferJob, typeof(TransferJob));
        }

        /// <summary>
        /// Gets a copy of this transfer object.
        /// </summary>
        /// <returns>A copy of current transfer object</returns>
        public SingleObjectTransfer Copy()
        {
            lock (this.ProgressTracker)
            {
                return new SingleObjectTransfer(this);
            }
        }

        /// <summary>
        /// Execute the transfer asynchronously.
        /// </summary>
        /// <param name="scheduler">Transfer scheduler</param>
        /// <param name="cancellationToken">Token that can be used to cancel the transfer.</param>
        /// <returns>A task representing the transfer operation.</returns>
        public override async Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            if (this.transferJob.Status == TransferJobStatus.Finished ||
                this.transferJob.Status == TransferJobStatus.Skipped)
            {
                return;
            }

            if (transferJob.Status == TransferJobStatus.Failed)
            {
                // Resuming a failed transfer job
                this.UpdateTransferJobStatus(transferJob, TransferJobStatus.Transfer);
            }

            try
            {
                await scheduler.ExecuteJobAsync(transferJob, cancellationToken);
                this.UpdateTransferJobStatus(transferJob, TransferJobStatus.Finished);
            }
            catch (TransferException exception)
            {
                if (exception.ErrorCode == TransferErrorCode.NotOverwriteExistingDestination)
                {
                    // transfer skipped
                    this.UpdateTransferJobStatus(transferJob, TransferJobStatus.Skipped);
                }
                else
                {
                    // transfer failed
                    this.UpdateTransferJobStatus(transferJob, TransferJobStatus.Failed);
                }

                throw;
            }
        }
    }
}
