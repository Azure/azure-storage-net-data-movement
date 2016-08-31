//------------------------------------------------------------------------------
// <copyright file="SingleObjectTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Represents a single object transfer operation.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class SingleObjectTransfer : Transfer
    {
        private const string TransferJobName = "TransferJob";

        /// <summary>
        /// Internal transfer job.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
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
            Debug.Assert(source != null && dest != null);
            Debug.Assert(
                source.Type == TransferLocationType.FilePath || source.Type == TransferLocationType.Stream ||
                source.Type == TransferLocationType.AzureBlob || source.Type == TransferLocationType.AzureFile ||
                source.Type == TransferLocationType.SourceUri);
            Debug.Assert(
                dest.Type == TransferLocationType.FilePath || dest.Type == TransferLocationType.Stream ||
                dest.Type == TransferLocationType.AzureBlob || dest.Type == TransferLocationType.AzureFile ||
                dest.Type == TransferLocationType.SourceUri);
            Debug.Assert(!((source.Type == TransferLocationType.FilePath || source.Type == TransferLocationType.Stream) &&
                (dest.Type == TransferLocationType.FilePath || dest.Type == TransferLocationType.Stream)));

            if (source.Type == TransferLocationType.AzureBlob && dest.Type == TransferLocationType.AzureBlob)
            {
                CloudBlob sourceBlob = (source as AzureBlobLocation).Blob;
                CloudBlob destBlob = (dest as AzureBlobLocation).Blob;
                if (sourceBlob.BlobType != destBlob.BlobType)
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationBlobTypeDifferent);
                }

                if (StorageExtensions.Equals(sourceBlob, destBlob))
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
                }
            }

            if (source.Type == TransferLocationType.AzureFile && dest.Type == TransferLocationType.AzureFile)
            {
                CloudFile sourceFile = (source as AzureFileLocation).AzureFile;
                CloudFile destFile = (dest as AzureFileLocation).AzureFile;
                if (string.Equals(sourceFile.Uri.Host, destFile.Uri.Host, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(sourceFile.Uri.AbsolutePath, destFile.Uri.AbsolutePath, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(Resources.SourceAndDestinationLocationCannotBeEqualException);
                }
            }

            this.transferJob = new TransferJob(this);
        }

#if !BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a deserialized SingleObjectTransfer
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            this.transferJob.Transfer = this;
        }
#endif

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SingleObjectTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected SingleObjectTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.transferJob = (TransferJob)info.GetValue(TransferJobName, typeof(TransferJob));
            this.transferJob.Transfer = this;
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleObjectTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="SingleObjectTransfer"/> object. </param>
        private SingleObjectTransfer(SingleObjectTransfer other)
            : base(other)
        {
            this.ProgressTracker = other.ProgressTracker.Copy();
            this.transferJob = other.transferJob.Copy();
            this.transferJob.Transfer = this;
        }

#if BINARY_SERIALIZATION
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
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Creates a copy of current transfer object.
        /// </summary>
        /// <returns>A copy of current transfer object.</returns>
        public SingleObjectTransfer Copy()
        {
            return new SingleObjectTransfer(this);
        }

        public void UpdateProgressLock(ReaderWriterLockSlim uploadLock)
        {
            this.transferJob.ProgressUpdateLock = uploadLock;
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

            TransferEventArgs eventArgs = new TransferEventArgs(this.Source.ToString(), this.Destination.ToString());
            eventArgs.StartTime = DateTime.UtcNow;

            if (this.transferJob.Status == TransferJobStatus.Failed)
            {
                // Resuming a failed transfer job
                if (string.IsNullOrEmpty(this.transferJob.CopyId))
                {
                    this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Transfer);
                }
                else
                {
                    this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Monitor);
                }
            }

            try
            {
                await scheduler.ExecuteJobAsync(this.transferJob, cancellationToken);
                this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Finished);

                eventArgs.EndTime = DateTime.UtcNow;

                if(this.Context != null)
                {
                    this.Context.OnTransferSuccess(eventArgs);
                }
            }
            catch (TransferException exception)
            {
                eventArgs.EndTime = DateTime.UtcNow;
                eventArgs.Exception = exception;

                if (exception.ErrorCode == TransferErrorCode.NotOverwriteExistingDestination)
                {
                    // transfer skipped
                    this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Skipped);
                    if (this.Context != null)
                    {
                        this.Context.OnTransferSkipped(eventArgs);
                    }

                    throw;
                }
                else
                {
                    // transfer failed
                    this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Failed);
                    if (this.Context != null)
                    {
                        this.Context.OnTransferFailed(eventArgs);
                    }

                    throw;
                }
            }
            catch (Exception ex)
            {
                eventArgs.EndTime = DateTime.UtcNow;
                eventArgs.Exception = ex;

                // transfer failed
                this.UpdateTransferJobStatus(this.transferJob, TransferJobStatus.Failed);

                if (this.Context != null)
                {
                    this.Context.OnTransferFailed(eventArgs);
                }

                throw;
            }
        }
    }
}
