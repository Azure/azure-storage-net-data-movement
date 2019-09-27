//------------------------------------------------------------------------------
// <copyright file="HierarchyDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;
    using Microsoft.Azure.Storage.File;

    /// <summary>
    /// Represents a hierarchy directory transfer operation.
    /// In a flat directory transfer, the enumeration only returns file entries and it only transfers files under the directory.
    /// 
    /// In a hierarchy directory transfer, the enumeration also returns directory entries, 
    /// it transfers files under the directory and also handles opertions on directories.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    class HierarchyDirectoryTransfer : DirectoryTransfer
    {
        private const string OngoingSubDirectoriesCountName = "OngoingSubDirectoriesCount";
        private const string OngoingSubDirectoryName = "OngoingSubDirectory";
        private const string SubDirectoriesCountName = "SubDirectoriesCount";
        private const string SubDirectoryName = "SubDirectory";
        private const string EnumerationstartedName = "Enumerationstarted";

        /// <summary>
        /// Serialization field name for sub transfers.
        /// </summary>
        private const string SubTransfersName = "SubTransfers";

        long outstandingTasks = 1;

        private TaskCompletionSource<object> transfersCompleteSource = null;
        private TaskCompletionSource<object> subDirTransfersCompleteSource = null;

        /// <summary>
        /// Relative path of listed subdirectores.
        /// </summary>
        private ConcurrentQueue<string> subDirectories = new ConcurrentQueue<string>();

        private ConcurrentDictionary<SubDirectoryTransfer, object> ongoingSubDirTransfers
            = new ConcurrentDictionary<SubDirectoryTransfer, object>();

        // This is to control maximum count of file (single transfer instance) put into TransferScheduler.
        private SemaphoreSlim maxConcurrencyControl = null;

        private int maxConcurrency = 0;

        object continuationTokenLock = new object();

        private Exception enumerateException = null;
        private CancellationTokenSource cancellationTokenSource = null;

        private ReaderWriterLockSlim progressUpdateLock = new ReaderWriterLockSlim();

        private TransferCollection<SingleObjectTransfer> subTransfers;

        private TransferScheduler currentScheduler = null;
        private CancellationToken currentCancellationToken = CancellationToken.None;

        // This is notify the main execute thread for directory that there's new directory item listed.
        private ManualResetEventSlim newAddSubDirResetEventSlim = new ManualResetEventSlim();

        private DirectoryListingScheduler directoryListingScheduler = null;

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private int enumerationStarted = 0;

        public HierarchyDirectoryTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
            this.subTransfers = new TransferCollection<SingleObjectTransfer>();
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Instances will be disposed in other place.")]
        protected HierarchyDirectoryTransfer(HierarchyDirectoryTransfer other)
            : base(other)
        {
            other.progressUpdateLock?.EnterWriteLock();
            this.ProgressTracker = other.ProgressTracker.Copy();
            lock (other.continuationTokenLock)
            {
                foreach (var subDirTransfer in other.subDirectories)
                {
                    this.subDirectories.Enqueue(subDirTransfer);
                }

                foreach (var subDirTransfer in other.ongoingSubDirTransfers)
                {
                    var newSubDirTransfer = new SubDirectoryTransfer(subDirTransfer.Key);
                    this.ongoingSubDirTransfers.TryAdd(newSubDirTransfer, new object());
                }

                // copy transfers
                this.subTransfers = other.subTransfers.Copy();
                this.enumerationStarted = other.enumerationStarted;
            }
            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
            other.progressUpdateLock?.ExitWriteLock();
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchyDirectoryTransfer"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected HierarchyDirectoryTransfer(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.enumerationStarted = info.GetInt32(EnumerationstartedName);

            if (context.Context is StreamJournal)
            {
                this.subTransfers = new TransferCollection<SingleObjectTransfer>();
            }
            else
            {
                this.subTransfers = (TransferCollection<SingleObjectTransfer>)info.GetValue(SubTransfersName, typeof(TransferCollection<SingleObjectTransfer>));

                long count = info.GetInt64(OngoingSubDirectoriesCountName);

                for (int i = 0; i < count; ++i)
                {
                    this.ongoingSubDirTransfers.TryAdd(
                        (SubDirectoryTransfer)info.GetValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", OngoingSubDirectoryName, i), typeof(SubDirectoryTransfer)),
                        new object());
                }

                count = info.GetInt64(SubDirectoriesCountName);
                for (int i = 0; i < count; ++i)
                {
                    this.subDirectories.Enqueue((string)info.GetValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", SubDirectoryName, i), typeof(string)));
                }
            }

            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(EnumerationstartedName, this.enumerationStarted);

            if (!(context.Context is StreamJournal))
            {
                // serialize sub transfers
                info.AddValue(SubTransfersName, this.subTransfers, typeof(TransferCollection<SingleObjectTransfer>));

                long count = this.ongoingSubDirTransfers.Count;
                info.AddValue(OngoingSubDirectoriesCountName, count);
                int index = 0;
                foreach (var subDirTransfer in this.ongoingSubDirTransfers)
                {
                    info.AddValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", OngoingSubDirectoryName, index), subDirTransfer.Key, typeof(SubDirectoryTransfer));
                    ++index;
                }

                count = this.subDirectories.Count;
                info.AddValue(SubDirectoriesCountName, count);
                index = 0;
                foreach (var subDirTransfer in this.subDirectories)
                {
                    info.AddValue(string.Format(CultureInfo.InvariantCulture, "{0}{1}", SubDirectoryName, index), subDirTransfer, typeof(string));
                    ++index;
                }
            }
        }
#endif // BINARY_SERIALIZATION

#region Serialization helpers

#if !BINARY_SERIALIZATION
        [DataMember]
        private TransferCollection<SingleObjectTransfer> serializedSubTransfers;

        [DataMember]
        private SubDirectoryTransfer[] serializedOngoingDirTransfers;

        [DataMember]
        private string[] serializedSubDirectories;

        /// <summary>
        /// Initializes a deserialized HierarchyDirectoryTransfer (by rebuilding the the transfer
        /// dictionary and progress tracker)
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            // Constructors and field initializers are not called by DCS, so initialize things here
            progressUpdateLock = new ReaderWriterLockSlim();
            continuationTokenLock = new object();

            if (!IsStreamJournal)
            {
                this.subTransfers = this.serializedSubTransfers;
                this.ongoingSubDirTransfers = new ConcurrentDictionary<SubDirectoryTransfer, object>();

                foreach (var ongoingSubDirTransfer in this.serializedOngoingDirTransfers)
                {
                    this.ongoingSubDirTransfers.TryAdd(ongoingSubDirTransfer, new object());
                }

                this.subDirectories = new ConcurrentQueue<string>();

                foreach (var subdirectory in this.serializedSubDirectories)
                {
                    this.subDirectories.Enqueue(subdirectory);
                }
            }
            else
            {
                this.subTransfers = new TransferCollection<SingleObjectTransfer>();
                this.ongoingSubDirTransfers = new ConcurrentDictionary<SubDirectoryTransfer, object>();
                this.subDirectories = new ConcurrentQueue<string>();
            }

            this.subTransfers.OverallProgressTracker.Parent = this.ProgressTracker;
            this.outstandingTasks = 1;
        }

        /// <summary>
        /// Serializes the object by storing the trasnfers in a more DCS-friendly format
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            if (!IsStreamJournal)
            {
                this.serializedSubTransfers = this.subTransfers;
                this.serializedOngoingDirTransfers = this.ongoingSubDirTransfers.Keys.ToArray();
                this.serializedSubDirectories = this.subDirectories.ToArray();
            }
        }
#endif //!BINARY_SERIALIZATION
#endregion // Serialization helpers

        public override int MaxTransferConcurrency
        {
            set
            {
                Debug.Assert((value > 1), "MaxTransferConcurrency cannot be smaller than 1");
                Debug.Assert(null == this.maxConcurrencyControl, "MaxTransferConcurrency can only be set once");

                base.MaxTransferConcurrency = value;
                if (null == this.maxConcurrencyControl)
                {
                    this.maxConcurrencyControl = new SemaphoreSlim(value + 1, value + 1);
                    this.maxConcurrency = value;
                }
            }
        }

        public string SearchPattern
        {
            get;
            set;
        }

        public bool Recursive
        {
            get;
            set;
        }

        public bool FollowSymblink
        {
            get;
            set;
        }

        public override Transfer Copy()
        {
            return new HierarchyDirectoryTransfer(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.subDirTransfersCompleteSource)
                {
                    this.subDirTransfersCompleteSource.Task.Wait();
                }

                if (null != this.transfersCompleteSource)
                {
                    this.transfersCompleteSource.Task.Wait();
                }

                if (null != this.cancellationTokenSource)
                {
                    this.cancellationTokenSource.Dispose();
                    this.cancellationTokenSource = null;
                }

                if (null != this.progressUpdateLock)
                {
                    this.progressUpdateLock.Dispose();
                    this.progressUpdateLock = null;
                }

                if (null != this.maxConcurrencyControl)
                {
                    this.maxConcurrencyControl.Dispose();
                    this.maxConcurrencyControl = null;
                }

                if (null != this.newAddSubDirResetEventSlim)
                {
                    this.newAddSubDirResetEventSlim.Dispose();
                    this.newAddSubDirResetEventSlim = null;
                }

                if (null != this.directoryListingScheduler)
                {
                    this.directoryListingScheduler.Dispose();
                    this.directoryListingScheduler = null;
                }
            }

            base.Dispose(disposing);
        }

        private void ResetExecutionStatus()
        {
            if (null != this.cancellationTokenSource)
            {
                this.cancellationTokenSource.Dispose();
            }

            if (null != this.newAddSubDirResetEventSlim)
            {
                this.newAddSubDirResetEventSlim.Dispose();
            }

            if (null != directoryListingScheduler)
            {
                this.directoryListingScheduler.Dispose();
            }

            this.newAddSubDirResetEventSlim = new ManualResetEventSlim();
            this.cancellationTokenSource = new CancellationTokenSource();
            this.transfersCompleteSource = new TaskCompletionSource<object>();
            this.subDirTransfersCompleteSource = new TaskCompletionSource<object>();

#if DOTNET5_4
            int maxListingThreadCount = 6;
#else
            int maxListingThreadCount = 2;
#endif

            if ((this.Destination.Type == TransferLocationType.LocalDirectory) || (this.Source.Type == TransferLocationType.LocalDirectory))
            {
#if DOTNET5_4
                maxListingThreadCount = 4;
#else
                maxListingThreadCount = 2;
#endif
            }

            directoryListingScheduler = new DirectoryListingScheduler(maxListingThreadCount);
        }

        public override async Task ExecuteInternalAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Yield();
                this.ResetExecutionStatus();

                cancellationToken.Register(() =>
                {
                    this.cancellationTokenSource?.Cancel();
                });

                this.currentScheduler = scheduler;
                this.currentCancellationToken = cancellationToken;

                this.maxConcurrencyControl.Wait();

                await DoEnumerationAndTransferAsync(scheduler, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                if (null == this.enumerateException)
                {
                    throw;
                }
            }
            finally
            {
                this.SignalSubDirTaskDecrement();
                await this.subDirTransfersCompleteSource.Task;

                if (this.maxConcurrency == this.maxConcurrencyControl.Release())
                {
                    this.transfersCompleteSource.TrySetResult(null);
                }

                await this.transfersCompleteSource.Task;
            }

            if (null != this.enumerateException)
            {
                throw this.enumerateException;
            }
            else
            {
                Utils.CheckCancellation(cancellationToken);
            }
        }

        internal void AddSubDir(string relativePath, Func<SerializableListContinuationToken> updateContinuationToken)
        {
            if (null != this.Journal)
            {
                SerializableListContinuationToken continuationToken = updateContinuationToken();
                this.Journal.AddSubDirTransfer(relativePath);
                this.Journal.UpdateJournalItem(continuationToken);
            }
            else
            {
                lock (this.continuationTokenLock)
                {
                    this.subDirectories.Enqueue(relativePath);
                    updateContinuationToken();
                }
            }

            newAddSubDirResetEventSlim.Set();
        }

        internal void AddSingleObjectTransfer(
            SingleObjectTransfer singleObjectTransfer,
            Func<SerializableListContinuationToken> updateContinuationToken)
        {
            SerializableListContinuationToken listContinuationToken = null;

            // Add to subtransfers in the TransferCollection for checkpoint (journal without stream)
            lock (this.continuationTokenLock)
            {
                this.subTransfers.AddTransfer(singleObjectTransfer);
                listContinuationToken = updateContinuationToken();
            }

            if (null != this.Journal)
            {
                this.Journal.AddSubtransfer(singleObjectTransfer);
                this.Journal.UpdateJournalItem(listContinuationToken);
            }

            this.TransferFile(singleObjectTransfer, this.currentScheduler, this.currentCancellationToken);
        }

        internal async void TransferFile(SingleObjectTransfer transferItem, TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            try
            {
                this.maxConcurrencyControl.Wait(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // No need to report exception here, OperationCanceledException will be handled in other place.
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            await Task.Yield();
            bool hasError = false;
            bool shouldStopTransfer = false;

            transferItem.UpdateProgressLock(this.progressUpdateLock);

            try
            {
                using (transferItem)
                {
                    await transferItem.ExecuteAsync(scheduler, cancellationToken);
                }
            }
            catch (TransferException ex)
            {
                if (ex.ErrorCode == TransferErrorCode.FailedCheckingShouldTransfer)
                {
                    shouldStopTransfer = true;
                    this.enumerateException = new TransferException(
                        TransferErrorCode.FailToEnumerateDirectory,
                        string.Format(CultureInfo.CurrentCulture,
                            Resources.EnumerateDirectoryException,
                            this.Destination.Instance.ConvertToString()), 
                        ex.InnerException);
                }

                hasError = true;
            }
            catch
            {
                hasError = true;
            }
            finally
            {
                if ((!hasError)
                   || (null != this.Journal))
                {
                    this.subTransfers.RemoveTransfer(transferItem);
                }

                if (this.maxConcurrency == this.maxConcurrencyControl.Release())
                {
                    this.transfersCompleteSource.TrySetResult(null);
                }
            }

            if (shouldStopTransfer)
            {
                this.cancellationTokenSource?.Cancel();
            }
        }

        private bool Resume(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            bool isResume = false;
            if (null != this.Journal)
            {
                foreach (var transfer in this.Journal.ListSubTransfers())
                {
                    isResume = true;
                    Utils.CheckCancellation(this.cancellationTokenSource.Token);
                    transfer.Context = this.Context;

                    this.UpdateTransfer(transfer);

                    this.subTransfers.AddTransfer(transfer, false);
                    this.TransferFile((transfer as SingleObjectTransfer), scheduler, cancellationToken);
                }

                foreach (var subDirTransfer in this.Journal.ListSubDirTransfers())
                {
                    isResume = true;
                    Utils.CheckCancellation(this.cancellationTokenSource.Token);
                    subDirTransfer.Update(this);

                    this.ScheduleSubDirectoryTransfer(
                          subDirTransfer,
                          this.cancellationTokenSource.Token,
                          () =>
                          { },
                          -1);
                }
            }
            else
            {
                // return all existing transfers in subTransfers
                foreach (var transfer in this.subTransfers.GetEnumerator())
                {
                    isResume = true;
                    Utils.CheckCancellation(this.cancellationTokenSource.Token);
                    transfer.Context = this.Context;

                    this.UpdateTransfer(transfer);
                    this.TransferFile((transfer as SingleObjectTransfer), scheduler, cancellationToken);
                }

                foreach (var subDirTransferPair in this.ongoingSubDirTransfers)
                {
                    isResume = true;
                    var subDirTransfer = subDirTransferPair.Key;
                    subDirTransfer.Update(this);

                    this.ScheduleSubDirectoryTransfer(
                            subDirTransfer,
                            this.cancellationTokenSource.Token,
                            () =>
                            {
                            },
                            -1);
                }
            }

            return isResume;
        }

        private async Task DoEnumerationAndTransferAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            await Task.Yield();

            this.Resume(scheduler, cancellationToken);

            if (0 == this.enumerationStarted)
            {
                // Got nothing from checkpoint, start directory transfer from the very beginning.
                var subDirTransfer = new SubDirectoryTransfer(this, "");

                lock (this.continuationTokenLock)
                {
                    if (null == this.Journal)
                    {
                        this.ongoingSubDirTransfers.TryAdd(subDirTransfer, new object());
                    }
                    this.enumerationStarted = 1;
                }

                if (null != this.Journal)
                {
                    this.Journal.AddOngoingSubDirTransfer(subDirTransfer);
                    this.Journal?.UpdateJournalItem(this);
                }

                this.ScheduleSubDirectoryTransfer(
                        subDirTransfer,
                        this.cancellationTokenSource.Token,
                        null,
                        -1);
            }

            bool gotDirectory = true;
            while (true)
            {
                Utils.CheckCancellation(this.cancellationTokenSource.Token);

                if (!gotDirectory)
                {
                    newAddSubDirResetEventSlim.Wait(cancellationToken);
                    newAddSubDirResetEventSlim.Reset();
                }

                // Check whether theres ongoing subdirectory listing thread.
                bool listCompleted = (1 == Interlocked.Read(ref this.outstandingTasks));

                string subDirRelativePath = null;
                if (null != this.Journal)
                {
                    subDirRelativePath = this.Journal.PeekSubDirTransfer();
                }
                else
                {
                    this.subDirectories.TryPeek(out subDirRelativePath);
                }

                if (string.IsNullOrEmpty(subDirRelativePath))
                {
                    if (listCompleted)
                    {
                        // There's no ongoing subdirectory listing thread,
                        // and no subdirectory pending on listing
                        // This means that the whole listing is completed.
                        break;
                    }

                    gotDirectory = false;
                    continue;
                }
                else
                {
                    gotDirectory = true;
                    Utils.CheckCancellation(this.cancellationTokenSource.Token);
                    SubDirectoryTransfer subDirTransfer = new SubDirectoryTransfer(this, subDirRelativePath);

                    this.ScheduleSubDirectoryTransfer(
                            subDirTransfer,
                            this.cancellationTokenSource.Token,
                            () =>
                            {
                                if (null != this.Journal)
                                {
                                    this.Journal.AddOngoingSubDirTransfer(subDirTransfer);
                                    this.Journal.RemoveFirstSubDirTransfer();
                                }
                                else
                                {
                                    this.ongoingSubDirTransfers.TryAdd(subDirTransfer, new object());
                                    this.subDirectories.TryDequeue(out subDirRelativePath);
                                }
                            },
                            -1);
                }
            }
        }

        private void SignalSubDirTaskDecrement()
        {
            long currentTaskCount = Interlocked.Decrement(ref this.outstandingTasks);

            if (1 == currentTaskCount)
            {
                this.newAddSubDirResetEventSlim.Set();
            }
            else if (0 == currentTaskCount)
            {
                this.subDirTransfersCompleteSource.SetResult(null);
            }
        }

        private bool ScheduleSubDirectoryTransfer(
            SubDirectoryTransfer subDirectoryTransfer,
            CancellationToken cancellationToken,
            Action persistDirTransfer,
            int timeOut)
        {
            Interlocked.Increment(ref this.outstandingTasks);
            Task directoryListTask = null;
            try
            {
                directoryListTask = this.directoryListingScheduler.Schedule(
                    subDirectoryTransfer,
                    cancellationToken,
                    persistDirTransfer,
                    timeOut);
            }
            catch (OperationCanceledException)
            {
                // Ignore exception
            }
            catch (ObjectDisposedException)
            {
                // Ignore exception
            }

            this.WaitOnSubDirectoryListTask(directoryListTask, subDirectoryTransfer);
            return null != directoryListTask;
        }

        private async void WaitOnSubDirectoryListTask(Task directoryListTask, SubDirectoryTransfer subDirTransfer)
        {
            if (null == directoryListTask)
            {
                this.SignalSubDirTaskDecrement();
            }
            else
            {
                bool shouldStopOthers = false;
                bool errorHappened = false;
                try
                {
                    await directoryListTask;
                }
                catch (OperationCanceledException)
                {
                    // Ingore this exception, there's other place reporting such kind of exception when cancellation is triggered.
                    errorHappened = true;
                }
                catch (Exception ex)
                {
                    if (ex is TransferException)
                    {
                        this.enumerateException = ex;
                    }
                    else
                    {
                        this.enumerateException = new TransferException(
                            TransferErrorCode.FailToEnumerateDirectory,
                            string.Format(CultureInfo.CurrentCulture,
                                Resources.EnumerateDirectoryException,
                                this.Destination.Instance.ConvertToString()), 
                            ex);
                    }

                    shouldStopOthers = true;
                    errorHappened = true;
                }
                finally
                {
                    if (!errorHappened)
                    {
                        if (null != this.Journal)
                        {
                            this.Journal.RemoveSubDirTransfer(subDirTransfer);
                        }
                        else
                        {
                            object subDirTransferValue = null;
                            this.ongoingSubDirTransfers.TryRemove(subDirTransfer, out subDirTransferValue);
                        }
                    }

                    this.SignalSubDirTaskDecrement();
                }

                if (shouldStopOthers)
                {
                    this.cancellationTokenSource?.Cancel();
                }
            }
        }

        internal void GetSubDirLocation(string relativePath, out TransferLocation sourceLocation, out TransferLocation destLocation)
        {
            if (string.IsNullOrEmpty(relativePath))
            {
                sourceLocation = this.Source;
                destLocation = this.Destination;
                return;
            }
            else
            {
                var transferEntry = CreateDirectoryTransferEntry(relativePath);
                sourceLocation = GetSourceDirectoryTransferLocation(this.Source, relativePath);
                destLocation = GetDestinationSubDirTransferLocation(this.Destination, transferEntry);
            }
        }

        protected TransferEntry CreateDirectoryTransferEntry(string relativePath)
        {
            if (this.Source.Type == TransferLocationType.AzureFileDirectory)
            {
                return new AzureFileDirectoryEntry(
                    relativePath,
                    (this.Source as AzureFileDirectoryLocation).FileDirectory.GetDirectoryReference(relativePath),
                    null);
            }
            else if (this.Source.Type == TransferLocationType.LocalDirectory)
            {
                return new DirectoryEntry
                    (relativePath, 
                    LongPath.Combine(this.Source.Instance as string, relativePath), null);
            }
            else
            {
                // For now, HierarchyDirectoryTransfer should only be used when source is Azure File Directory.
                throw new ArgumentException("TransferLocationType");
            }
        }

        protected static TransferLocation GetSourceDirectoryTransferLocation(TransferLocation dirLocation, string relativePath)
        {
            if (dirLocation.Type == TransferLocationType.AzureFileDirectory)
            {
                AzureFileDirectoryLocation azureFileDirLocation = dirLocation as AzureFileDirectoryLocation;
                var destDirectory = azureFileDirLocation.FileDirectory.GetDirectoryReference(relativePath);

                AzureFileDirectoryLocation azureFileLocation = new AzureFileDirectoryLocation(destDirectory);
                azureFileLocation.FileRequestOptions = azureFileDirLocation.FileRequestOptions;

                return azureFileLocation;
            }
            else if (dirLocation.Type == TransferLocationType.LocalDirectory)
            {
                DirectoryLocation localDirLocation = dirLocation as DirectoryLocation;
                var destDirectory = Path.Combine(localDirLocation.DirectoryPath, relativePath);

                DirectoryLocation localSubDirLocation = new DirectoryLocation(destDirectory);

                return localSubDirLocation;
            }
            else
            {
                // For now, HierarchyDirectoryTransfer should only be used when source is Azure File Directory.
                throw new ArgumentException("TransferLocationType");
            }
        }

        protected TransferLocation GetDestinationSubDirTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            string destRelativePath = this.NameResolver.ResolveName(entry);

            switch (dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    {
                        AzureBlobDirectoryLocation blobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                        BlobType destBlobType = this.BlobType;

                        // TODO: should handle blob type here.
                        AzureBlobDirectoryLocation retLocation = new AzureBlobDirectoryLocation(blobDirLocation.BlobDirectory.GetDirectoryReference(destRelativePath));
                        retLocation.BlobRequestOptions = blobDirLocation.BlobRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.AzureFileDirectory:
                    {
                        AzureFileDirectoryLocation fileDirLocation = dirLocation as AzureFileDirectoryLocation;
                        CloudFileDirectory azureDirectory = fileDirLocation.FileDirectory.GetDirectoryReference(destRelativePath);

                        AzureFileDirectoryLocation retLocation = new AzureFileDirectoryLocation(azureDirectory);
                        retLocation.FileRequestOptions = fileDirLocation.FileRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.LocalDirectory:
                    {
                        DirectoryLocation localDirLocation = dirLocation as DirectoryLocation;
                        string path = LongPath.Combine(localDirLocation.DirectoryPath, destRelativePath);

                        return new DirectoryLocation(path);
                    }

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }
    }
}
