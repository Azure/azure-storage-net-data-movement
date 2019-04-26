//------------------------------------------------------------------------------
// <copyright file="ReadDataState.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System.IO;

    /// <summary>
    /// Keep the state of reading a single block from the input stream. 
    /// </summary>
    internal class ReadDataState : TransferDataState
    {
        /// <summary>
        /// Gets or sets the memory stream used to encapsulate the memory
        /// buffer for passing the methods such as PutBlock, WritePages, 
        /// DownloadToStream and DownloadRangeToStream, as these methods
        /// requires a stream and doesn't allow for a byte array as input.
        /// </summary>
        public Stream MemoryStream
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the memory manager that controls global memory
        /// allocation.
        /// </summary>
        public MemoryManager MemoryManager
        {
            get;
            set;
        }

        /// <summary>
        /// Private dispose method to release managed/unmanaged objects.
        /// If disposing = true clean up managed resources as well as unmanaged resources.
        /// If disposing = false only clean up unmanaged resources.
        /// </summary>
        /// <param name="disposing">Indicates whether or not to dispose managed resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.MemoryStream)
                {
                    this.MemoryStream.Dispose();
                    this.MemoryStream = null;
                }

                if (null != this.MemoryBuffer)
                {
                    this.MemoryManager.ReleaseBuffers(this.MemoryBuffer);
                    this.MemoryManager = null;
                }
            }
        }
    }
}
