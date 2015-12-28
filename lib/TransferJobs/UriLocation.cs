//------------------------------------------------------------------------------
// <copyright file="UriLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    internal class UriLocation : TransferLocation, ISerializable
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

        private UriLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.Uri = (Uri)info.GetValue(UriName, typeof(Uri));
        }

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
        public Uri Uri
        {
            get;
            private set;
        }

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
