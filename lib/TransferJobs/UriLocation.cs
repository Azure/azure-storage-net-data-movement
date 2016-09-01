//------------------------------------------------------------------------------
// <copyright file="UriLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class UriLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string UriName = "Uri";

        /// <summary>
        /// Initializes a new instance of the <see cref="UriLocation"/> class.
        /// </summary>
        /// <param name="uri">Uri to the source in an asynchronously copying job.</param>
        public UriLocation(Uri uri)
        {
            if (null == uri)
            {
                throw new ArgumentNullException("uri");
            }

            this.Uri = uri;
        }

#if BINARY_SERIALIZATION
        private UriLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.Uri = (Uri)info.GetValue(UriName, typeof(Uri));
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.SourceUri;
            }
        }

        /// <summary>
        /// Gets Uri to the location.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public Uri Uri
        {
            get;
            private set;
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            info.AddValue(UriName, this.Uri, typeof(Uri));
        }
#endif // BINARY_SERIALIZATION

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
            return this.Uri.ToString();
        }
    }
}
