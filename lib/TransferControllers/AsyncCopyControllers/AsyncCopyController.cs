//------------------------------------------------------------------------------
// <copyright file="AsyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.Blob.Protocol;
    using Microsoft.Azure.Storage.DataMovement.Extensions;
    using Microsoft.Azure.Storage.File;

    internal abstract class AsyncCopyController : TransferControllerBase
    {
        /// <summary>
        /// Timer to signal refresh status.
        /// </summary>
        private Timer statusRefreshTimer;

        /// <summary>
        /// Lock to protect statusRefreshTimer.
        /// </summary>
        private object statusRefreshTimerLock = new object();

        /// <summary>  
        /// Wait time between two status refresh requests.  
        /// </summary>  
        private long statusRefreshWaitTime = Constants.CopyStatusRefreshMinWaitTimeInMilliseconds;

        /// <summary>
        /// Indicates whether the copy job is apporaching finish.
        /// </summary>
        private bool approachingFinish = false;

        /// <summary>
        /// Request count sent with current statusRefreshWaitTime
        /// </summary>
        private long statusRefreshRequestCount = 0;

        /// <summary>
        /// Keeps track of the internal state-machine state.
        /// </summary>
        private volatile State state;

        /// <summary>
        /// Indicates whether the controller has work available
        /// or not for the calling code. 
        /// </summary>
        private bool hasWork;

        /// <summary>
        /// Indicates the BytesCopied value of last CopyState
        /// </summary>
        private long lastBytesCopied;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncCopyController"/> class.
        /// </summary>
        /// <param name="scheduler">Scheduler object which creates this object.</param>
        /// <param name="transferJob">Instance of job to start async copy.</param>
        /// <param name="userCancellationToken">Token user input to notify about cancellation.</param>
        internal AsyncCopyController(
            TransferScheduler scheduler,
            TransferJob transferJob,
            CancellationToken userCancellationToken)
            : base(scheduler, transferJob, userCancellationToken)
        {
            if (null == transferJob.Destination)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "Dest"),
                    "transferJob");
            }

            switch(this.TransferJob.Source.Type)
            {
                case TransferLocationType.AzureBlob:
                    this.SourceBlob = (this.TransferJob.Source as AzureBlobLocation).Blob;
                    break;

                case TransferLocationType.AzureFile:
                    this.SourceFile = (this.TransferJob.Source as AzureFileLocation).AzureFile;
                    break;

                case TransferLocationType.SourceUri:
                    this.SourceUri = (this.TransferJob.Source as UriLocation).Uri;
                    break;

                default:
                    throw new ArgumentException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.ProvideExactlyOneOfThreeParameters,
                            "Source.SourceUri",
                            "Source.Blob",
                            "Source.AzureFile"),
                        "transferJob");
            }

            // initialize the status refresh timer
            this.statusRefreshTimer = new Timer(
                new TimerCallback(
                    delegate(object timerState)
                    {
                        this.hasWork = true;
#if DOTNET5_4
                    }), null, -1, Timeout.Infinite);
#else
                }));
#endif

            this.SetInitialStatus();
        }

        /// <summary>
        /// Internal state values.
        /// </summary>
        private enum State
        {
            FetchSourceAttributes,
            GetDestination,
            StartCopy,
            GetCopyState,
            Finished,
            Error,
        }

        public override bool HasWork
        {
            get
            {
                return this.hasWork;
            }
        }

        protected CloudBlob SourceBlob
        {
            get;
            private set;
        }

        protected CloudFile SourceFile
        {
            get;
            private set;
        }

        protected Uri SourceUri
        {
            get;
            private set;
        }

        protected abstract Uri DestUri
        {
            get;
        }

        /// <summary>
        /// Do work in the controller.
        /// A controller controls the whole transfer from source to destination, 
        /// which could be split into several work items. This method is to let controller to do one of those work items.
        /// There could be several work items to do at the same time in the controller. 
        /// </summary>
        /// <returns>Whether the controller has completed. This is to tell <c>TransferScheduler</c> 
        /// whether the controller can be disposed.</returns>
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
                case State.StartCopy:
                    await this.StartCopyAsync();
                    break;
                case State.GetCopyState:
                    await this.GetCopyStateAsync();
                    break;
                case State.Finished:
                case State.Error:
                default:
                    break;
            }

            return (State.Error == this.state || State.Finished == this.state);
        }

        /// <summary>
        /// Sets the state of the controller to Error, while recording
        /// the last occurred exception and setting the HasWork and 
        /// IsFinished fields.
        /// </summary>
        /// <param name="ex">Exception to record.</param>
        protected override void SetErrorState(Exception ex)
        {
            Debug.Assert(
                this.state != State.Finished,
                "SetErrorState called, while controller already in Finished state");

            this.state = State.Error;
            this.hasWork = false;
        }

        /// <summary>
        /// Taken from <c>Microsoft.Azure.Storage.Core.Util.HttpUtility</c>: Parse the http query string.
        /// </summary>
        /// <param name="query">Http query string.</param>
        /// <returns>A dictionary of query pairs.</returns>
        protected static Dictionary<string, string> ParseQueryString(string query)
        {
            Dictionary<string, string> retVal = new Dictionary<string, string>();
            if (query == null || query.Length == 0)
            {
                return retVal;
            }

            // remove ? if present
            if (query.StartsWith("?", StringComparison.OrdinalIgnoreCase))
            {
                query = query.Substring(1);
            }

            string[] valuePairs = query.Split(new string[] { "&" }, StringSplitOptions.RemoveEmptyEntries);

            foreach (string vp in valuePairs)
            {
                int equalDex = vp.IndexOf("=", StringComparison.OrdinalIgnoreCase);
                if (equalDex < 0)
                {
                    retVal.Add(Uri.UnescapeDataString(vp), null);
                    continue;
                }

                string key = vp.Substring(0, equalDex);
                string value = vp.Substring(equalDex + 1);

                retVal.Add(Uri.UnescapeDataString(key), Uri.UnescapeDataString(value));
            }

            return retVal;
        }

        private void SetInitialStatus()
        {
            switch (this.TransferJob.Status)
            {
                case TransferJobStatus.NotStarted:
                    this.TransferJob.Status = TransferJobStatus.Transfer;
                    break;
                case TransferJobStatus.Transfer:
                    break;
                case TransferJobStatus.Monitor:
                    this.lastBytesCopied = this.TransferJob.Transfer.ProgressTracker.BytesTransferred;
                    break;
                case TransferJobStatus.Finished:
                default:
                    throw new ArgumentException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.InvalidInitialEntryStatusForControllerException,
                        this.TransferJob.Status,
                        this.GetType().Name));
            }

            this.SetHasWorkAfterStatusChanged();
        }

        private void SetHasWorkAfterStatusChanged()
        {
            if (TransferJobStatus.Transfer == this.TransferJob.Status)
            {
                if (null != this.SourceUri)
                {
                    this.state = State.GetDestination;
                }
                else
                {
                    this.state = State.FetchSourceAttributes;
                }
            }
            else if(TransferJobStatus.Monitor == this.TransferJob.Status)
            {
                this.state = State.GetCopyState;
            }
            else
            {
                Debug.Fail("We should never be here");
            }

            this.hasWork = true;
        }

        private async Task FetchSourceAttributesAsync()
        {
            Debug.Assert(
                this.state == State.FetchSourceAttributes,
                "FetchSourceAttributesAsync called, but state isn't FetchSourceAttributes");

            this.hasWork = false;
            this.StartCallbackHandler();

            try
            {
                await this.DoFetchSourceAttributesAsync();
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

            if (this.TransferJob.Source.Type == TransferLocationType.AzureBlob)
            {
                (this.TransferJob.Source as AzureBlobLocation).CheckedAccessCondition = true;
            }
            else
            {
                (this.TransferJob.Source as AzureFileLocation).CheckedAccessCondition = true;
            }

            this.state = State.GetDestination;
            this.hasWork = true;
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

        private async Task GetDestinationAsync()
        {
            Debug.Assert(
                this.state == State.GetDestination,
                "GetDestinationAsync called, but state isn't GetDestination");

            this.hasWork = false;
            this.StartCallbackHandler();

            if (!this.IsForceOverwrite)
            {
                try
                {
                    await this.DoFetchDestAttributesAsync();
                }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
                {
                    var se = e as StorageException ?? e.InnerException as StorageException;
#else
                catch (StorageException se)
                {
#endif
                    if (!await this.HandleGetDestinationResultAsync(se))
                    {
                        throw se;
                    }
                    return;
                }
            }

            await this.HandleGetDestinationResultAsync(null);
        }

        private async Task<bool> HandleGetDestinationResultAsync(Exception e)
        {
            bool destExist = !this.IsForceOverwrite;

            if (null != e)
            {
                StorageException se = e as StorageException;

                // Getting a storage exception is expected if the destination doesn't
                // exist. In this case we won't error out, but set the 
                // destExist flag to false to indicate we will copy to 
                // a new blob/file instead of overwriting an existing one.
                if (null != se &&
                    null != se.RequestInformation &&
                    se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    destExist = false;
                }
                else
                {
                    this.DoHandleGetDestinationException(se);
                    return false;
                }
            }

            if (this.TransferJob.Destination.Type == TransferLocationType.AzureBlob)
            {
                (this.TransferJob.Destination as AzureBlobLocation).CheckedAccessCondition = true;
            }
            else if(this.TransferJob.Destination.Type == TransferLocationType.AzureFile)
            {
                (this.TransferJob.Destination as AzureFileLocation).CheckedAccessCondition = true;
            }

            if ((TransferJobStatus.Monitor == this.TransferJob.Status)
                && string.IsNullOrEmpty(this.TransferJob.CopyId))
            {
                throw new InvalidOperationException(Resources.RestartableInfoCorruptedException);
            }

            if (!this.IsForceOverwrite)
            {
                Uri sourceUri = this.GetSourceUri();

                // If destination file exists, query user whether to overwrite it.
                await this.CheckOverwriteAsync(
                    destExist,
                    sourceUri.ToString(),
                    this.DestUri.ToString());
            }

            this.UpdateProgressAddBytesTransferred(0);

            this.state = State.StartCopy;

            this.hasWork = true;
            return true;
        }

        private async Task StartCopyAsync()
        {
            Debug.Assert(
                this.state == State.StartCopy,
                "StartCopyAsync called, but state isn't StartCopy");

            this.hasWork = false;

            StorageCopyState copyState = null;

            try
            {
                copyState = await this.DoStartCopyAsync();
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
            {
                var se = e as StorageException ?? e.InnerException as StorageException;
#else
            catch (StorageException se)
            {
#endif
                if (!this.HandleStartCopyResult(se))
                {
                    throw;
                }

                return;
            }

            this.TransferJob.CopyId = copyState.CopyId;

            if ((copyState.Status == StorageCopyStatus.Success) && copyState.TotalBytes.HasValue)
            {
                await this.HandleFetchCopyStateResultAsync(copyState, false);
            }
            else
            {
                this.HandleStartCopyResult(null);
            }
        }

        private bool HandleStartCopyResult(StorageException se)
        {
            if (null != se)
            {
                if (null != se.RequestInformation
                && BlobErrorCodeStrings.PendingCopyOperation == se.RequestInformation.ErrorCode)
                {
                    StorageCopyState copyState = this.FetchCopyStateAsync().Result;

                    if (null == copyState)
                    {
                        return false;
                    }

                    string baseUriString = copyState.Source.GetComponents(
                        UriComponents.Host | UriComponents.Port | UriComponents.Path, UriFormat.UriEscaped);

                    Uri sourceUri = this.GetSourceUri();

                    string ourBaseUriString = sourceUri.GetComponents(UriComponents.Host | UriComponents.Port | UriComponents.Path, UriFormat.UriEscaped);

                    DateTimeOffset? baseSnapshot = null;
                    DateTimeOffset? ourSnapshot = null == this.SourceBlob ? null : this.SourceBlob.SnapshotTime;

                    string snapshotString;
                    if (ParseQueryString(copyState.Source.Query).TryGetValue("snapshot", out snapshotString))
                    {
                        if (!string.IsNullOrEmpty(snapshotString))
                        {
                            DateTimeOffset snapshotTime;
                            if (DateTimeOffset.TryParse(
                                snapshotString,
                                CultureInfo.CurrentCulture,
                                DateTimeStyles.AdjustToUniversal,
                                out snapshotTime))
                            {
                                baseSnapshot = snapshotTime;
                            }
                        }
                    }

                    if (!baseUriString.Equals(ourBaseUriString) ||
                        !baseSnapshot.Equals(ourSnapshot))
                    {
                        return false;
                    }

                    if (string.IsNullOrEmpty(this.TransferJob.CopyId))
                    {
                        this.TransferJob.CopyId = copyState.CopyId;
                    }
                }
                else
                {
                    return false;
                }
            }

            this.TransferJob.Status = TransferJobStatus.Monitor;
            this.state = State.GetCopyState;
            this.TransferJob.Transfer.UpdateJournal();
            this.hasWork = true;
            return true;
        }

        private async Task GetCopyStateAsync()
        {
            Debug.Assert(
                this.state == State.GetCopyState,
                "GetCopyStateAsync called, but state isn't GetCopyState");

            this.hasWork = false;
            this.StartCallbackHandler();

            StorageCopyState copyState = null;

            try
            {
                copyState = await this.FetchCopyStateAsync();
            }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
            catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
            {
                var se = e as StorageException ?? e.InnerException as StorageException;
#else
            catch (StorageException se)
            {
#endif
                if (null != se.RequestInformation &&
                       se.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    // The reason of 404 (Not Found) may be that the destination blob has not been created yet.
                    this.RestartTimer();
                }
                else
                {
                    throw;
                }
            }

            await this.HandleFetchCopyStateResultAsync(copyState);
        }

        // In this method, it may need to set customized properties to destination.
        // If this method is invoked just after StartCopyAsync, 
        // properties on destination instance may not be totally the same with the one on server.
        // If this is the case, it should try to fetch attributes from server first.
        private async Task HandleFetchCopyStateResultAsync(
            StorageCopyState copyState, 
            bool gotDestinationAttributes = true)
        {
            if (null == copyState)
            {
                // Reach here, the destination should already exist.
                string exceptionMessage = string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.FailedToRetrieveCopyStateForObjectException,
                            this.DestUri.ToString());

                throw new TransferException(
                        TransferErrorCode.FailToRetrieveCopyStateForObject,
                        exceptionMessage);
            }
            else
            {
                // Verify we are monitoring the right blob copying process.
                if (!this.TransferJob.CopyId.Equals(copyState.CopyId))
                {
                    throw new TransferException(
                            TransferErrorCode.MismatchCopyId,
                            Resources.MismatchFoundBetweenLocalAndServerCopyIdsException);
                }

                if (StorageCopyStatus.Success == copyState.Status)
                {
                    this.UpdateTransferProgress(copyState);

                    this.DisposeStatusRefreshTimer();

                    if (null != this.TransferContext && null != this.TransferContext.SetAttributesCallbackAsync)
                    {
                        if (!gotDestinationAttributes)
                        {
                            await this.DoFetchDestAttributesAsync();
                        }

                        // If got here, we've done FetchAttributes on destination after copying completed on server,
                        // no need to one more round of FetchAttributes anymore.
                        await this.SetAttributesAsync(this.TransferContext.SetAttributesCallbackAsync);
                    }

                    this.SetFinished();
                }
                else if (StorageCopyStatus.Pending == copyState.Status)
                {
                    this.UpdateTransferProgress(copyState);

                    // Wait a period to restart refresh the status.
                    this.RestartTimer();
                }
                else
                {
                    string exceptionMessage = string.Format(
                                CultureInfo.CurrentCulture,
                                Resources.FailedToAsyncCopyObjectException,
                                this.GetSourceUri().ToString(),
                                this.DestUri.ToString(),
                                copyState.Status.ToString(),
                                copyState.StatusDescription);

                    // CopyStatus.Invalid | Failed | Aborted
                    throw new TransferException(
                            TransferErrorCode.AsyncCopyFailed,
                            exceptionMessage);
                }
            }
        }

        private void UpdateTransferProgress(StorageCopyState copyState)
        {
            if (null != copyState &&
                copyState.TotalBytes.HasValue)
            {
                Debug.Assert(
                    copyState.BytesCopied.HasValue,
                    "BytesCopied cannot be null as TotalBytes is not null.");

                if (this.approachingFinish == false &&
                    copyState.TotalBytes - copyState.BytesCopied <= Constants.CopyApproachingFinishThresholdInBytes)
                {
                    this.approachingFinish = true;
                }

                if (this.TransferContext != null)
                {
                    long bytesTransferred = copyState.BytesCopied.Value;

                    this.UpdateProgress(() =>
                    {
                        this.UpdateProgressAddBytesTransferred(bytesTransferred - this.lastBytesCopied);
                    });

                    this.lastBytesCopied = bytesTransferred;
                }
            }
        }

        private void SetFinished()
        {
            this.state = State.Finished;
            this.hasWork = false;

            this.FinishCallbackHandler(null);
        }

        private void RestartTimer()
        {
            if (this.approachingFinish)
            {
                this.statusRefreshWaitTime = Constants.CopyStatusRefreshMinWaitTimeInMilliseconds;
            }
            else if (this.statusRefreshRequestCount >= Constants.CopyStatusRefreshWaitTimeMaxRequestCount &&
                this.statusRefreshWaitTime < Constants.CopyStatusRefreshMaxWaitTimeInMilliseconds)
            {
                this.statusRefreshRequestCount = 0;
                this.statusRefreshWaitTime *= 10;
                this.statusRefreshWaitTime = Math.Min(this.statusRefreshWaitTime, Constants.CopyStatusRefreshMaxWaitTimeInMilliseconds);
            }
            else if (this.statusRefreshWaitTime < Constants.CopyStatusRefreshMaxWaitTimeInMilliseconds)
            {
                this.statusRefreshRequestCount++;
            }

            // Wait a period to restart refresh the status.
            this.statusRefreshTimer.Change(
                TimeSpan.FromMilliseconds(this.statusRefreshWaitTime),
                new TimeSpan(-1));
        }

        private void DisposeStatusRefreshTimer()
        {
            if (null != this.statusRefreshTimer)
            {
                lock (this.statusRefreshTimerLock)
                {
                    if (null != this.statusRefreshTimer)
                    {
                        this.statusRefreshTimer.Dispose();
                        this.statusRefreshTimer = null;
                    }
                }
            }
        }

        private Uri GetSourceUri()
        {
            if (null != this.SourceUri)
            {
                return this.SourceUri;
            }

            if (null != this.SourceBlob)
            {
                return this.SourceBlob.SnapshotQualifiedUri;
            }

            return this.SourceFile.SnapshotQualifiedUri;
        }

        protected async Task DoFetchSourceAttributesAsync()
        {
            if (this.TransferJob.Source.Type == TransferLocationType.AzureBlob)
            {
                AzureBlobLocation sourceLocation = this.TransferJob.Source as AzureBlobLocation;
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    sourceLocation.AccessCondition,
                    sourceLocation.CheckedAccessCondition);
                OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);

                await sourceLocation.Blob.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateBlobRequestOptions(sourceLocation.BlobRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
            else if(this.TransferJob.Source.Type == TransferLocationType.AzureFile)
            {
                AzureFileLocation sourceLocation = this.TransferJob.Source as AzureFileLocation;
                AccessCondition accessCondition = Utils.GenerateConditionWithCustomerCondition(
                    sourceLocation.AccessCondition,
                    sourceLocation.CheckedAccessCondition);
                OperationContext operationContext = Utils.GenerateOperationContext(this.TransferContext);
                await sourceLocation.AzureFile.FetchAttributesAsync(
                    accessCondition,
                    Utils.GenerateFileRequestOptions(sourceLocation.FileRequestOptions),
                    operationContext,
                    this.CancellationToken);
            }
        }

        protected abstract Task DoFetchDestAttributesAsync();
        protected abstract Task<StorageCopyState> DoStartCopyAsync();
        protected abstract void DoHandleGetDestinationException(StorageException se);
        protected abstract Task<StorageCopyState> FetchCopyStateAsync();
        protected abstract Task SetAttributesAsync(SetAttributesCallbackAsync setAttributes);
    }
}
