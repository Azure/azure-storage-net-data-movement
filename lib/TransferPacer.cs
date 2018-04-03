namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers;
    using System;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// <c>TransferPacer</c> takes in performance and state data and adjusts transfer parameters
    /// This class will be a central point for the tuning of performance attributes
    /// </summary>
    internal class TransferPacer
    {
        /// <summary>
        /// Transfer options that this manager will pass to transfer controllers.
        /// </summary>
        public TransferConfigurations Configurations { get; set; }
      
        /// <summary>
        /// Registers a <c>SyncTransferController</c> to be tracked by, and factored into pacing adjustments
        /// </summary>
        /// <remarks>
        /// Must occur before the controller is active
        /// </remarks>
        public void Register(SyncTransferController controller)
        {
            // Add the length to our total volume. 
            // This is liely 0, but reasonably could be set before calling Register
            var sharedData = controller.SharedTransferData;
            Interlocked.Add(ref totalScheduledVolume, sharedData.TotalLength);
            
            sharedData.TransferDataAdded += dataAddedHandler;
            sharedData.TotalLengthChanged += lengthChangedHandler;
        }

        /// <summary>
        /// Deregisters a registered <c>SyncTransferController</c>
        /// </summary>
        /// <remarks>
        /// Must occur after the controller has stopped (i.e. No more events will be received)
        /// </remarks>
        public void Deregister(SyncTransferController controller)
        {
            // Subtract from scheduled volume any unread data
            var sharedData = controller.SharedTransferData;
            Interlocked.Add(ref totalScheduledVolume, sharedData.ReadLength - sharedData.TotalLength);
            Debug.Assert(Interlocked.Read(ref totalScheduledVolume) >= 0);

            // Subtract buffered data from buffered volume
            foreach (var transferData in sharedData.Values)
            {
                Interlocked.Add(ref totalBufferedVolume, -(transferData.Length));
            }

            sharedData.TransferDataAdded -= dataAddedHandler;
            sharedData.TransferDataRemoved -= dataRemovedHandler;
            sharedData.TotalLengthChanged -= lengthChangedHandler;
        }
        
        /// <summary>
        /// The tuned range size to request for HTTP GET download requests
        /// </summary>
        public int RangeRequestSize {
            get
            {
                long value = Math.Min(
                    Interlocked.Read(ref this.totalScheduledVolume),
                    Configurations.MaximumCacheSize);
                value /= Configurations.ParallelOperations;
                value = Math.Max(Constants.MinBlockSize, Math.Min(value, Constants.MaxBlockSize));
                return (int)value;
            }
        }
          
        private long totalScheduledVolume = 0;
        private long totalBufferedVolume = 0;
        
        private void dataAddedHandler(object sender, TransferDataEventArgs e)
        {
            if (e.Success)
            {
                Interlocked.Add(ref totalScheduledVolume, -(e.Data.Length));
                Interlocked.Add(ref totalBufferedVolume, e.Data.Length);
            }
            Debug.Assert(Interlocked.Read(ref totalScheduledVolume) >= 0);
        }

        private void dataRemovedHandler(object sender, TransferDataEventArgs e)
        {
            if (e.Success)
            {
                Interlocked.Add(ref totalBufferedVolume, -(e.Data.Length));
            }
            Debug.Assert(Interlocked.Read(ref totalBufferedVolume) >= 0);
        }
        
        private void lengthChangedHandler(object sender, ValueChangeEventArgs<long> e)
        {
            Interlocked.Add(ref totalScheduledVolume, e.New - e.Old);
            Debug.Assert(Interlocked.Read(ref totalScheduledVolume) >= 0);
        }
    }
}