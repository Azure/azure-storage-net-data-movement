//-----------------------------------------------------------------------------
// <copyright file="TransferEventArgs.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;

    /// <summary>
    /// Transfer event args.
    /// </summary>
    public sealed class TransferEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferEventArgs"/> class.
        /// </summary>
        /// <param name="source">String representation of transfer source location.</param>
        /// <param name="destination">String representation of transfer destination location.</param>
        public TransferEventArgs(string source, string destination)
        {
            this.Source = source;
            this.Destination = destination;
        }

        /// <summary>
        /// Gets the string representation of transfer source location.
        /// </summary>
        public string Source
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the string representation of transfer destination location.
        /// </summary>
        public string Destination
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets transfer start time.
        /// </summary>
        public DateTime StartTime
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets transfer end time.
        /// </summary>
        public DateTime EndTime
        {
            get;
            internal set;
        }

        /// <summary>
        /// Gets the exception if the transfer is failed, or null if the transfer is success.
        /// </summary>
        public Exception Exception
        {
            get;
            internal set;
        }
    }
}
