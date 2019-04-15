//------------------------------------------------------------------------------
// <copyright file="SerializableTransferLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(AzureFileDirectoryLocation))]
    [KnownType(typeof(AzureFileLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
    internal sealed class SerializableTransferLocation
#if BINARY_SERIALIZATION
        : ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string TransferLocationTypeName = "LocationType";
        private const string TransferLocationName = "Location";

        public SerializableTransferLocation(TransferLocation location)
        {
            this.Location = location;
        }

#if BINARY_SERIALIZATION
        private SerializableTransferLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            TransferLocationType locationType = (TransferLocationType)info.GetValue(TransferLocationTypeName, typeof(TransferLocationType));
            switch (locationType)
            { 
                case TransferLocationType.FilePath:
                    this.Location = (FileLocation)info.GetValue(TransferLocationName, typeof(FileLocation));
                    break;
                case TransferLocationType.Stream:
                    throw new InvalidOperationException(Resources.CannotSerializeStreamLocation);
                case TransferLocationType.SourceUri:
                    this.Location = (UriLocation)info.GetValue(TransferLocationName, typeof(UriLocation));
                    break;
                case TransferLocationType.AzureBlob:
                    this.Location = (AzureBlobLocation)info.GetValue(TransferLocationName, typeof(AzureBlobLocation));
                    break;
                case TransferLocationType.AzureFile:
                    this.Location = (AzureFileLocation)info.GetValue(TransferLocationName, typeof(AzureFileLocation));
                    break;
                case TransferLocationType.AzureBlobDirectory:
                    this.Location = (AzureBlobDirectoryLocation)info.GetValue(TransferLocationName, typeof(AzureBlobDirectoryLocation));
                    break;
                case TransferLocationType.AzureFileDirectory:
                    this.Location = (AzureFileDirectoryLocation)info.GetValue(TransferLocationName, typeof(AzureFileDirectoryLocation));
                    break;
                case TransferLocationType.LocalDirectory:
                    this.Location = (DirectoryLocation)info.GetValue(TransferLocationName, typeof(DirectoryLocation));
                    break;
                default:
                    break;
            }
        }
#endif // BINARY_SERIALIZATION

#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public TransferLocation Location
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

            info.AddValue(TransferLocationTypeName, this.Location.Type);

            switch(this.Location.Type)
            {
                case TransferLocationType.FilePath:
                    info.AddValue(TransferLocationName, this.Location as FileLocation, typeof(FileLocation));
                    break;
                case TransferLocationType.SourceUri:
                    info.AddValue(TransferLocationName, this.Location as UriLocation, typeof(UriLocation));
                    break;
                case TransferLocationType.AzureBlob:
                    info.AddValue(TransferLocationName, this.Location as AzureBlobLocation, typeof(AzureBlobLocation));
                    break;
                case TransferLocationType.AzureFile:
                    info.AddValue(TransferLocationName, this.Location as AzureFileLocation, typeof(AzureFileLocation));
                    break;
                case TransferLocationType.AzureBlobDirectory:
                    info.AddValue(TransferLocationName, this.Location as AzureBlobDirectoryLocation, typeof(AzureBlobDirectoryLocation));
                    break;
                case TransferLocationType.AzureFileDirectory:
                    info.AddValue(TransferLocationName, this.Location as AzureFileDirectoryLocation, typeof(AzureFileDirectoryLocation));
                    break;
                case TransferLocationType.LocalDirectory:
                    info.AddValue(TransferLocationName, this.Location as DirectoryLocation, typeof(DirectoryLocation));
                    break;
                default:
                    throw new InvalidOperationException(string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.CannotDeserializeLocationType,
                        this.Location.Type));
            }
        }
#endif // BINARY_SERIALIZATION
    }
}
