//------------------------------------------------------------------------------
// <copyright file="AzureFileListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    sealed class AzureFileListContinuationToken : ListContinuationToken
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string FilePathName = "FilePath";

        public AzureFileListContinuationToken(string filePath)
        {
            this.FilePath = filePath;
        }

#if BINARY_SERIALIZATION
        private AzureFileListContinuationToken(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.FilePath = info.GetString(FilePathName);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets relative path of the last listed file.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public string FilePath
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

            info.AddValue(FilePathName, this.FilePath, typeof(string));
        }
#endif // BINARY_SERIALIZATION
    }
}
