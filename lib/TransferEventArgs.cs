//-----------------------------------------------------------------------------
// <copyright file="TransferEventArgs.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
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
        /// <param name="source">Instance representation of transfer source location.</param>
        /// <param name="destination">Instance representation of transfer destination location.</param>
        public TransferEventArgs(object source, object destination)
        {
            this.Source = source;
            this.Destination = destination;
        }

        /// <summary>
        /// Gets the instance representation of transfer source location.
        /// </summary>
        public object Source
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the instance representation of transfer destination location.
        /// </summary>
        public object Destination
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
