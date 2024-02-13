//------------------------------------------------------------------------------
// <copyright file="FileLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class TransferItemLocation : FileLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string FilePathName = "FilePath";
        private const string RelativePathName = "RelativePath";

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferItemLocation"/> class.
        /// </summary>
        /// <param name="filePath">Path to the local file as a source/destination to be read from/written to in a transfer.</param>
        /// <param name="relativePath">Relative path to the local file as a source/destination to be read from/written to in a transfer.</param>
        public TransferItemLocation(string filePath, string relativePath) : base(filePath, relativePath)
        {
        }

#if BINARY_SERIALIZATION
        private TransferItemLocation(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.RelativePath = info.GetString(RelativePathName);
            this.FilePath = info.GetString(FilePathName);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.FilePath;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.FilePath;
            }
        }



        #region Serialization helpers
#if !BINARY_SERIALIZATION
        [DataMember] private string relativePath;

        /// <summary>
        /// Serializes the object by extracting FilePath for single object transfer.
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            fullPath = FilePath;
            relativePath = RelativePath;
        }

        /// <summary>
        /// Initializes a deserialized FilePath
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            FilePath = fullPath;
            RelativePath = relativePath;
        }
#endif // !BINARY_SERIALIZATION
        #endregion // Serialization helpers

#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            info.AddValue(RelativePathName, this.RelativePath, typeof(string));
            info.AddValue(FilePathName, this.FilePath, typeof(string));
        }
#endif // BINARY_SERIALIZATION
        

    }
}
