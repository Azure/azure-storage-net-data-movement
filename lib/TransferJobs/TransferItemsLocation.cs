//------------------------------------------------------------------------------
// <copyright file="DirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class TransferItemsLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private readonly object instance = new object();
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferItemsLocation"/> class.
        /// </summary>
        public TransferItemsLocation()
        {
            
        }

#if BINARY_SERIALIZATION
        private TransferItemsLocation(SerializationInfo info, StreamingContext context)
        {

        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.TransferItems;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance => instance;


#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            
        }
    }
}
