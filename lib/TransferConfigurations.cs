//------------------------------------------------------------------------------
// <copyright file="TransferConfigurations.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using ClientLibraryConstants = Microsoft.Azure.Storage.Shared.Protocol.Constants;

    /// <summary>
    /// TransferConfigurations class.
    /// </summary>
    public class TransferConfigurations
    {
        /// <summary>
        /// Stores the BlockSize to use for Windows Azure Storage transfers to block blob(s).
        /// It must be between 4MB and 100MB and be multiple of 4MB.
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
        /// How many listings can be performed in parallel.
        /// </summary>
        private int? maxListingConcurrency;

        /// <summary>
        /// Instance to call native methods to get current memory status.
        /// </summary>
        private GlobalMemoryStatusNativeMethods memStatus = new GlobalMemoryStatusNativeMethods();

        /// <summary>
        /// Initializes a new instance of the
        /// <see cref="TransferConfigurations" /> class.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public TransferConfigurations()
        {
            // setup default values.
            this.blockSize = Constants.DefaultTransferChunkSize;
            this.parallelOperations = Environment.ProcessorCount * 8;
            this.MemoryChunkSize = Constants.DefaultMemoryChunkSize;

            this.UpdateMaximumCacheSize(this.blockSize);
            this.SupportUncPath = false;

            if (Interop.CrossPlatformHelpers.IsWindows)
            {
                try
                {
                    LongPath.GetFullPath("\\\\?\\F:");
                    this.SupportUncPath = true;
                }
                catch (Exception)
                {
                }
            }
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
                this.UpdateMaximumCacheSize(this.blockSize);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how many listing works to process concurrently.
        /// When source is an Azure File directory or local file directory, DataMovement Library would list the directory in parallel.
        /// This value is to indicate the maximum number of listing works to process in parallel.
        /// </summary>
        /// <value>How many listing works to process concurrently.</value>
        public int? MaxListingConcurrency
        {
            get
            {
                return this.maxListingConcurrency;
            }
            set
            {
                if (value <= 0)
                    throw new ArgumentException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Resources.MaxListingConcurrencyNotPositiveException));
                this.maxListingConcurrency = value;
            }
        }

        /// <summary>
        /// Gets or sets the BlockSize to use for Windows Azure Storage transfers to block blob(s).
        /// It must be between 4MB and 100MB and be multiple of 4MB.
        ///
        /// Currently, the max block count of a block blob is limited to 50000.
        /// When transfering a big file and the BlockSize provided is smaller than the minimum value - (size/50000),
        /// it'll be reset to a value which is greater than the minimum value and multiple of 4MB for this file.
        /// </summary>
        /// <value>BlockSize to use for Windows Azure Storage transfers.</value>
        public int BlockSize
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

                if (value % Constants.DefaultTransferChunkSize != 0)
                {
                    throw new ArgumentException(Resources.BlockSizeMustBeMultipleOf4MB, "value");
                }

                this.blockSize = value;
                this.UpdateMaximumCacheSize(this.blockSize);
            }
        }

        /// <summary>
        /// Gets or sets the user agent prefix
        /// </summary>
        public string UserAgentPrefix
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
                if (value < Constants.DefaultTransferChunkSize)
                {
                    throw new ArgumentException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.SmallMemoryCacheSizeLimitationException,
                        Utils.BytesToHumanReadableSize(Constants.DefaultTransferChunkSize)));
                }

                if (0 == this.memStatus.AvailablePhysicalMemory)
                {
                    this.maximumCacheSize = Math.Min(value, Constants.MemoryCacheMaximum);
                }
                else
                {
#if DOTNET5_4
                    if (8 == Marshal.SizeOf(new IntPtr()))
#else
                    if (Environment.Is64BitProcess)
#endif
                    {
                        this.maximumCacheSize = Math.Min(value, (long) (this.memStatus.AvailablePhysicalMemory * Constants.MemoryCacheMultiplier));
                    }
                    else
                    {
                        this.maximumCacheSize = Math.Min(value,
                            Math.Min((long) (this.memStatus.AvailablePhysicalMemory*Constants.MemoryCacheMultiplier), Constants.MemoryCacheMaximum));
                    }
                }

                TransferManager.SetMemoryLimitation(this.maximumCacheSize);
            }
        }

        /// <summary>
        /// To indicate whether the process environment supports UNC path.
        /// 
        /// On Windows, it requires to use UNC path to access files/directories with long path. 
        /// In some environment, the .Net Framework only supports legacy path without UNC path support.
        /// DataMovement Library will detect whether .Net Framework supports UNC path in the process environment 
        /// to determine whether to use UNC path in the following transfers.
        /// </summary>
        internal bool SupportUncPath { get; private set; }

        /// <summary>
        /// The size of memory chunk of memory pool
        /// </summary>
        internal int MemoryChunkSize { get; private set; }

        /// <summary>
        /// Update the memory pool size according to the block size
        /// </summary>
        /// <param name="newBlockSize"></param>
        internal void UpdateMaximumCacheSize(int newBlockSize)
        {
            this.MaximumCacheSize = (long)3 * newBlockSize * this.ParallelOperations;
        }
    }
}
