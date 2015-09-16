//------------------------------------------------------------------------------
// <copyright file="TransferConfigurations.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.Reflection;
    using ClientLibraryConstants = Microsoft.WindowsAzure.Storage.Shared.Protocol.Constants;

    /// <summary>
    /// TransferConfigurations class.
    /// </summary>
    public class TransferConfigurations
    {
        /// <summary>
        /// Stores the BlockSize to use for Windows Azure Storage transfers.
        /// </summary>
        private int blockSize;

        /// <summary>
        /// How many work items to process in parallel.
        /// </summary>
        private int parallelOperations;

        /// <summary>
        /// Maximum amount of cache memory to use in bytes.
        /// </summary>
        private long maximumCacheSize; 

        /// <summary>
        /// Instance to call native methods to get current memory status.
        /// </summary>
        private GlobalMemoryStatusNativeMethods memStatus = new GlobalMemoryStatusNativeMethods();

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="TransferConfigurations" /> class.
        /// </summary>
        public TransferConfigurations()
        {
            // setup default values.
            this.ParallelOperations = Environment.ProcessorCount * 8;
            this.BlockSize = Constants.DefaultBlockSize;
        }

        /// <summary>
        /// Gets or sets a value indicating how many work items to process 
        /// concurrently. Downloading or uploading a single blob can consist 
        /// of a large number of work items.
        /// </summary>
        /// <value>How many work items to process concurrently.</value>
        public int ParallelOperations 
        {
            get
            {
                return this.parallelOperations;
            }

            set
            {
                if (value <= 0)
                {
                    throw new ArgumentException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParallelCountNotPositiveException));
                }

                this.parallelOperations = value;
                this.SetMaxMemoryCacheSize();
            }
        }

        /// <summary>
        /// Gets or sets the user agent suffix
        /// </summary>
        public string UserAgentSuffix
        {
            get;
            set; 
        }

        /// <summary>
        /// Gets or sets a value indicating how much memory we can cache
        /// during upload/download.
        /// </summary>
        /// <value>Maximum amount of cache memory to use in bytes.</value>
        internal long MaximumCacheSize
        {
            get
            {
                return this.maximumCacheSize;
            }

            set
            {
                if (value < Constants.MaxBlockSize)
                {
                    throw new ArgumentException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.SmallMemoryCacheSizeLimitationException,
                        Utils.BytesToHumanReadableSize(Constants.MaxBlockSize)));
                }

                this.maximumCacheSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the BlockSize to use for Windows Azure Storage transfers.
        /// </summary>
        /// <value>BlockSize to use for Windows Azure Storage transfers.</value>
        internal int BlockSize
        {
            get
            {
                return this.blockSize;
            }

            set
            {
                if (Constants.MinBlockSize > value || value > Constants.MaxBlockSize)
                {
                    string errorMessage = string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.BlockSizeOutOfRangeException,
                        Utils.BytesToHumanReadableSize(Constants.MinBlockSize),
                        Utils.BytesToHumanReadableSize(Constants.MaxBlockSize));

                    throw new ArgumentOutOfRangeException("value", value, errorMessage);
                }

                this.blockSize = value;
            }
        }

        private void SetMaxMemoryCacheSize()
        {
            if (0 == this.memStatus.AvailablePhysicalMemory)
            {
                this.MaximumCacheSize = Constants.CacheSizeMultiplierInByte * this.ParallelOperations;
            }
            else
            {
                this.MaximumCacheSize =
                    Math.Min(
                        Constants.CacheSizeMultiplierInByte * this.ParallelOperations,
                        Math.Min(
                            (long)(this.memStatus.AvailablePhysicalMemory * Constants.MemoryCacheMultiplier),
                            Constants.MemoryCacheMaximum));
            }
        }
    }
}
