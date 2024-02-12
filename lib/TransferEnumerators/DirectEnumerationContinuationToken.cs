//------------------------------------------------------------------------------
// <copyright file="FileListContinuationToken.cs" company="Microsoft">
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
    sealed class DirectEnumerationContinuationToken : FileListContinuationToken
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string SkipCountName = "SkipCount";

        public DirectEnumerationContinuationToken(int skipCount) : base(string.Empty)
        {
            this.SkipCount = skipCount;
        }

#if BINARY_SERIALIZATION
        private DirectEnumerationContinuationToken(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.SkipCount = info.GetInt32(SkipCountName);
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets relative path of the last listed file.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public int SkipCount
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

            info.AddValue(SkipCountName, this.SkipCount, typeof(int));
        }
#endif // BINARY_SERIALIZATION
    }
}
