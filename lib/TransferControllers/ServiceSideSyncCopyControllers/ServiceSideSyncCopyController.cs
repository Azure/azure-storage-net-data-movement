//------------------------------------------------------------------------------
// <copyright file="ServiceSideSyncCopyController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------


namespace Microsoft.Azure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;

    abstract class ServiceSideSyncCopyController : TransferControllerBase
    {
        /// <summary>
        /// Internal state values.
        /// </summary>
        protected enum State
        {
            FetchSourceAttributes, // To fetch source's attributes to check source existence and 
            GetDestination, // Check destination existence, check overwrite if destination exists, create destination blob/file if needed
            PreCopy, // Only work for page blob and file copying, to get ranges list to avoid copying empty ranges.
            Copy, // Copy content
            Commit, // For block blob, submit block list; For all types of blobs and file, copying source's attributes to destination.
            Finished,
            Error,
        }

        protected bool hasWork;
        protected State state;
        protected bool gotDestAttributes;

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

            this.state = State.FetchSourceAttributes;
        }

        public override bool HasWork => this.hasWork;

        protected ServiceSideSyncCopySource.ISourceHandler SourceHandler { get; set; }
        protected ServiceSideSyncCopyDest.IDestHandler DestHandler { get; set; }

        protected override async Task<bool> DoWorkInternalAsync()
        {
            if (!this.TransferJob.Transfer.ShouldTransferChecked)
            {
                this.hasWork = false;
                if (await this.CheckShouldTransfer().ConfigureAwait(false))
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
                    await this.FetchSourceAttributesAsync().ConfigureAwait(false);
                    break;
                case State.GetDestination:
                    await this.GetDestinationAsync().ConfigureAwait(false);
                    break;
                case State.PreCopy:
                    await this.DoPreCopyAsync().ConfigureAwait(false);
                    break;
                case State.Copy:
                    await this.CopyChunkAsync().ConfigureAwait(false);
                    break;
                case State.Commit:
                    await this.CommitAsync().ConfigureAwait(false);
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
            await this.SourceHandler.FetchAttributesAsync(this.CancellationToken).ConfigureAwait(false);
            this.PostFetchSourceAttributes();
        }

        protected async Task CheckAndCreateDestinationAsync()
        {
            this.gotDestAttributes = await this.DestHandler.CheckAndCreateDestinationAsync(
                this.IsForceOverwrite,
                this.SourceHandler.TotalLength,
                async (exist) =>
                {
                    await this.CheckOverwriteAsync(exist, this.SourceHandler.Uri, this.DestHandler.Uri).ConfigureAwait(false);
                },
                this.CancellationToken).ConfigureAwait(false);
        }

        protected async Task CommonCommitAsync()
        {
            await this.DestHandler.CommitAsync(gotDestAttributes, this.SourceHandler.SourceAttributes,
                this.SetCustomAttributesAsync,
                this.CancellationToken).ConfigureAwait(false);
        }

        protected abstract void PostFetchSourceAttributes();
        protected abstract Task GetDestinationAsync();
        protected abstract Task DoPreCopyAsync();
        protected abstract Task CopyChunkAsync();
        protected abstract Task CommitAsync();
    }
}
