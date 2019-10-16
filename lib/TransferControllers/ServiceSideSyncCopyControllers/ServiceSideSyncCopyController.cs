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
            await this.SourceHandler.FetchAttributesAsync(this.CancellationToken);
            this.PostFetchSourceAttributes();
        }

        protected async Task CheckAndCreateDestinationAsync()
        {
            this.gotDestAttributes = await this.DestHandler.CheckAndCreateDestinationAsync(
                this.IsForceOverwrite,
                this.SourceHandler.TotalLength,
                async (exist) =>
                {
                    await this.CheckOverwriteAsync(exist, this.SourceHandler.Uri, "fdsfds");
                },
                this.CancellationToken);
        }

        protected async Task CommonCommitAsync()
        {
            await this.DestHandler.CommitAsync(gotDestAttributes, this.SourceHandler.SourceAttributes,
                this.SetCustomAttributesAsync,
                this.CancellationToken);
        }

        protected abstract void PostFetchSourceAttributes();
        protected abstract Task GetDestinationAsync();
        protected abstract Task DoPreCopyAsync();
        protected abstract Task CopyChunkAsync();
        protected abstract Task CommitAsync();
    }
}
