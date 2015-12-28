//------------------------------------------------------------------------------
// <copyright file="StreamLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;

    internal class StreamLocation : TransferLocation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamLocation"/> class.
        /// </summary>
        /// <param name="stream">Stream instance as a source/destination to be read from/written to in a transfer.</param>
        public StreamLocation(Stream stream)
        {
            if (null == stream)
            {
                throw new ArgumentNullException("stream");
            }

            this.Stream = stream;
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.Stream;
            }
        }

        /// <summary>
        /// Gets an stream instance representing the location for this instance.
        /// </summary>
        public Stream Stream
        {
            get;
            private set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            return;
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.Stream.ToString();
        }

        // Summary:
        //     Determines whether the specified object is equal to the current stream location.
        //
        // Parameters:
        //   obj:
        //     The object to compare with the current stream location.
        //
        // Returns:
        //     true if the specified object is equal to the current stream location; otherwise, false.
        public override bool Equals(object obj)
        {
            return Object.ReferenceEquals(this, obj);
        }

        //
        // Summary:
        //     Returns the hash code for the transfer location.
        //
        // Returns:
        //     A 32-bit signed integer hash code.
        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }
    }
}
