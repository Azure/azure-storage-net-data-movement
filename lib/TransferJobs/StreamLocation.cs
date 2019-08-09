//------------------------------------------------------------------------------
// <copyright file="StreamLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;

#if !BINARY_SERIALIZATION
    [DataContract]
#endif
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

#if !BINARY_SERIALIZATION
        /// <summary>
        /// Deserializes the SerializableTransferLocation.
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            throw new InvalidOperationException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.CannotDeserializeLocationType,
                TransferLocationType.Stream));
        }

        /// <summary>
        /// Deserializes the SerializableTransferLocation.
        /// </summary>
        /// <param name="context"></param>
        [OnDeserializing]
        private void OnDeserializingCallback(StreamingContext context)
        {
            throw new InvalidOperationException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.CannotDeserializeLocationType,
                TransferLocationType.Stream));
        }
#endif

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
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.Stream;
            }
        }

        /// <summary>
        /// Gets a stream instance representing the location for this instance.
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
    }
}
