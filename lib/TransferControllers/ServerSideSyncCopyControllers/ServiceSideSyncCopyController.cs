using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    abstract class ServiceSideSyncCopyController : TransferControllerBase
    {
        /// <summary>
        /// Internal state values.
        /// </summary>
        protected enum State
        {
            FetchSourceAttributes,
            GetDestination,
            PreCopy,
            Copy,
            Commit,
            Finished,
            Error,
        }

        protected bool hasWork;
        protected State state;
        protected CloudBlob sourceBlob;
        protected CloudBlob destBlob;
        protected AzureBlobLocation sourceLocation;
        protected AzureBlobLocation destLocation;

        protected Attributes sourceAttributes;
        protected bool gotDestAttributes;
        protected long totalLength;

        protected ServiceSideSyncCopyController(
            TransferScheduler transferScheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
            if (null == transferJob.Destination)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "transferJob"),
                    "transferJob");
            }

            this.sourceLocation = this.TransferJob.Source as AzureBlobLocation;
            this.destLocation = this.TransferJob.Destination as AzureBlobLocation;
            this.sourceBlob = sourceLocation.Blob;
            this.destBlob = this.destLocation.Blob;

            this.state = State.FetchSourceAttributes;
        }

        public override bool HasWork => this.hasWork;

        protected override async Task<bool> DoWorkInternalAsync()
        {
            if (!this.TransferJob.Transfer.ShouldTransferChecked)
            {
                this.hasWork = false;
                if (await this.CheckShouldTransfer())
                {
                    return true;
                }
                else
                {
                    this.hasWork = true;
                    return false;
                }
            }

            switch (this.state)
            {
                case State.FetchSourceAttributes:
                    await this.FetchSourceAttributesAsync();
                    break;
                case State.GetDestination:
                    await this.GetDestinationAsync();
                    break;
                case State.PreCopy:
                    await this.DoPreCopyAsync();
                    break;
                case State.Copy:
                    await this.CopyChunkAsync();
                    break;
                case State.Commit:
                    await this.CommitAsync();
                    break;
                case State.Finished:
                case State.Error:
                default:
                    break;
            }

            return (State.Error == this.state || State.Finished == this.state);
        }

        protected async Task FetchSourceAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchSourceAttributes,
                "FetchSourceAttributesAsync called, but state isn't FetchSourceAttributes");

            this.hasWork = false;
            this.StartCallbackHandler();
            if (this.sourceLocation.IsInstanceInfoFetched != true)
            {
                AccessCondition accessCondition = Utils.GenerateIfMatchConditionWithCustomerCondition(
                     this.sourceLocation.ETag,
                     this.sourceLocation.AccessCondition,
                     this.sourceLocation.CheckedAccessCondition);
                try
                {
                    await this.sourceBlob.FetchAttributesAsync(
                        accessCondition,
                        Utils.GenerateBlobRequestOptions(this.sourceLocation.BlobRequestOptions),
                        Utils.GenerateOperationContext(this.TransferContext),
                        this.CancellationToken);
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception ex) when (ex is StorageException || (ex is AggregateException && ex.InnerException is StorageException))
            {
                var e = ex as StorageException ?? ex.InnerException as StorageException;
#else
                catch (StorageException e)
                {
#endif
                    HandleFetchSourceAttributesException(e);
                    throw;
                }

                this.sourceLocation.CheckedAccessCondition = true;
            }

            if (string.IsNullOrEmpty(this.sourceLocation.ETag))
            {
                if (0 != this.TransferJob.CheckPoint.EntryTransferOffset)
                {
                    throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
                }

                this.sourceLocation.ETag = this.sourceBlob.Properties.ETag;
            }
            else if ((this.TransferJob.CheckPoint.EntryTransferOffset > this.sourceBlob.Properties.Length)
                 || (this.TransferJob.CheckPoint.EntryTransferOffset < 0))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            this.sourceAttributes = Utils.GenerateAttributes(this.sourceBlob);

            this.totalLength = this.sourceBlob.Properties.Length;

            this.PostFetchSourceAttributes();
        }

        private static void HandleFetchSourceAttributesException(StorageException e)
        {
            // Getting a storage exception is expected if the source doesn't
            // exist. For those cases that indicate the source doesn't exist
            // we will set a specific error state.
            if (e?.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException(Resources.SourceDoesNotExistException, e);
            }
        }

        protected async Task CheckAndCreateDestinationAsync()
        {
            bool needCreateDestination = true;
            if (!this.IsForceOverwrite)
            {
                if (this.TransferJob.Overwrite.HasValue)
                {
                    if (!this.TransferJob.Overwrite.Value)
                    {
                        string exceptionMessage = string.Format(CultureInfo.InvariantCulture, Resources.OverwriteCallbackCancelTransferException, this.sourceBlob.Uri.ToString(), this.destBlob.Uri.ToString());
                        throw new TransferSkippedException(exceptionMessage);
                    }
                }
                else if (!this.destLocation.CheckedAccessCondition && null != this.destLocation.AccessCondition)
                {
                    try
                    {
                        await this.destBlob.FetchAttributesAsync(
                            Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, false),
                            Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions),
                            Utils.GenerateOperationContext(this.TransferContext),
                            this.CancellationToken);

                        // Only try to send the blob creating request, when blob length is not as expected. Otherwise, only need to clear all pages.
                        needCreateDestination = (this.destBlob.Properties.Length != this.totalLength);
                        this.destLocation.CheckedAccessCondition = true;
                        this.gotDestAttributes = true;

                        await this.CheckOverwriteAsync(
                            true,
                            this.sourceBlob.Uri.ToString(),
                            this.destBlob.Uri.ToString());
                    }
                    catch (StorageException se)
                    {
                        if ((null == se.RequestInformation) || ((int)HttpStatusCode.NotFound != se.RequestInformation.HttpStatusCode))
                        {
                            throw;
                        }
                    }
                }
                else
                {
                    AccessCondition accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");

                    try
                    {
                        await this.CreateDestinationAsync(accessCondition, CancellationToken.None);

                        needCreateDestination = false;
                        this.destLocation.CheckedAccessCondition = true;
                        this.TransferJob.Overwrite = true;
                        this.TransferJob.Transfer.UpdateJournal();
                    }
                    catch (StorageException se)
                    {
                        if ((null != se.RequestInformation) &&
                            (((int)HttpStatusCode.Conflict == se.RequestInformation.HttpStatusCode)
                                    && string.Equals(se.RequestInformation.ErrorCode, "BlobAlreadyExists")))
                        {
                            await this.CheckOverwriteAsync(
                                true,
                                this.sourceBlob.Uri.ToString(),
                                this.destBlob.Uri.ToString());
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
            else
            {
                this.gotDestAttributes = true;
            }

            Utils.CheckCancellation(this.CancellationToken);

            if (needCreateDestination)
            {
                await this.CreateDestinationAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition, this.destLocation.CheckedAccessCondition),
                    this.CancellationToken);

                this.TransferJob.Overwrite = true;
                this.destLocation.CheckedAccessCondition = true;
                this.TransferJob.Transfer.UpdateJournal();
            }
        }

        protected async Task CommonCommitAsync()
        {
            BlobRequestOptions blobRequestOptions = Utils.GenerateBlobRequestOptions(this.destLocation.BlobRequestOptions);
            OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);

            if (!this.gotDestAttributes)
            {
                await this.destBlob.FetchAttributesAsync(
                     Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                     blobRequestOptions,
                     operationContext,
                     this.CancellationToken);
            }

            var originalMetadata = new Dictionary<string, string>(this.destBlob.Metadata);

            Utils.SetAttributes(this.destBlob, this.sourceAttributes);

            await this.SetCustomAttributesAsync(this.destBlob);

            await this.destBlob.SetPropertiesAsync(
                Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                blobRequestOptions,
                operationContext,
                this.CancellationToken);

            if (!originalMetadata.DictionaryEquals(this.destBlob.Metadata))
            {
                await this.destBlob.SetMetadataAsync(
                    Utils.GenerateConditionWithCustomerCondition(this.destLocation.AccessCondition),
                    blobRequestOptions,
                    operationContext,
                    this.CancellationToken);
            }
        }

        protected abstract void PostFetchSourceAttributes();
        protected abstract Task CreateDestinationAsync(AccessCondition accessCondition, CancellationToken cancellationToken);
        protected abstract Task GetDestinationAsync();
        protected abstract Task DoPreCopyAsync();
        protected abstract Task CopyChunkAsync();
        protected abstract Task CommitAsync();
    }
}
