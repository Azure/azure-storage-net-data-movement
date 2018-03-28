namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers;
    using System;
    using System.Diagnostics;
    using System.Threading;

    internal class TransferPacer
    {
        /// <summary>
        /// Transfer options that this manager will pass to transfer controllers.
        /// </summary>
        public TransferConfigurations Configurations { get; set; }
      
        // Must occur before the controller is active
        public void Register(SyncTransferController controller)
        {
            // Add the length to our total volume. 
            // This is liely 0, but reasonably could be set before calling Register
            var sharedData = controller.SharedTransferData;
            Interlocked.Add(ref totalScheduledVolume, sharedData.TotalLength);
            
            sharedData.TransferDataAdded += dataAddedHandler;
            sharedData.TotalLengthChanged += lengthChangedHandler;
        }

        // Must occur after the controller has stopped
        // (i.e. No more events will be received)
        public void Deregister(SyncTransferController controller)
        {
            // Subtract from scheduled volume any unread data
            var sharedData = controller.SharedTransferData;
            Interlocked.Add(ref totalScheduledVolume, sharedData.ReadLength - sharedData.TotalLength);
            Debug.Assert(totalScheduledVolume >= 0);

            // Subtract buffered data from buffered volume
            foreach (var transferData in sharedData.Values)
            {
                Interlocked.Add(ref totalBufferedVolume, -(transferData.Length));
            }

            sharedData.TransferDataAdded -= dataAddedHandler;
            sharedData.TransferDataRemoved -= dataRemovedHandler;
            sharedData.TotalLengthChanged -= lengthChangedHandler;
        }
        
        public int RangeRequestSize {
            get
            {
                var value = this.totalScheduledVolume / Configurations.ParallelOperations;
                return (int)Math.Max(Constants.MinBlockSize, Math.Min(value, Constants.MaxBlockSize));
            }
        }

        public bool RequestHold
        {
            get
            {
                return Configurations.MaximumCacheSize - totalBufferedVolume < RangeRequestSize;
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
            Debug.Assert(totalScheduledVolume >= 0);
        }

        private void dataRemovedHandler(object sender, TransferDataEventArgs e)
        {
            if (e.Success)
            {
                Interlocked.Add(ref totalBufferedVolume, -(e.Data.Length));
            }
            Debug.Assert(totalBufferedVolume >= 0);
        }
        
        private void lengthChangedHandler(object sender, ValueChangeEventArgs<long> e)
        {
            Interlocked.Add(ref totalScheduledVolume, e.New - e.Old);
            Debug.Assert(totalScheduledVolume >= 0);
        }
    }
}