//------------------------------------------------------------------------------
// <copyright file="SerializableAccessCondition.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal sealed class SerializableAccessCondition
#if BINARY_SERIALIZATION
        : ISerializable
#endif // BINARY_SERIALIZATION
    {
        private const string IfMatchETagName = "IfMatchETag";
        private const string IfModifiedSinceTimeName = "IfModifiedSinceTime";
        private const string IfNoneMatchETagName = "IfNoneMatchETag";
        private const string IfNotModifiedSinceTimeName = "IfNotModifiedSinceTime";
        private const string IfSequenceNumberEqualName = "IfSequenceNumberEqual";
        private const string IfSequenceNumberLessThanName = "IfSequenceNumberLessThan";
        private const string IfSequenceNumberLessThanOrEqualName = "IfSequenceNumberLessThanOrEqual";
        private const string LeaseIdName = "LeaseId";
        
        private AccessCondition accessCondition;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableAccessCondition" /> class.
        /// </summary>
        public SerializableAccessCondition()
        {
        }

        #region Serialization helpers

#if !BINARY_SERIALIZATION
        [DataMember]
        private string ifMatchETag;
        
        [DataMember]
        private DateTimeOffset? ifModifiedSinceTime;
        
        [DataMember]
        private string ifNoneMatchETag;
        
        [DataMember]
        private DateTimeOffset? ifNotModifiedSinceTime;
        
        [DataMember]
        private long? ifSequenceNumberEqual;
        
        [DataMember]
        private long? ifSequenceNumberLessThan;
        
        [DataMember]
        private long? ifSequenceNumberLessThanOrEqual;
        
        [DataMember]
        private string leaseId;
        
        /// <summary>
        /// Serializes the object by extracting key data from the underlying AccessCondition
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            ifMatchETag = null == accessCondition ? null : this.accessCondition.IfMatchETag;
            ifModifiedSinceTime = null == accessCondition ? null : this.accessCondition.IfModifiedSinceTime;
            ifNoneMatchETag = null == accessCondition ? null : this.accessCondition.IfNoneMatchETag;
            ifNotModifiedSinceTime = null == accessCondition ? null : this.accessCondition.IfNotModifiedSinceTime;
            ifSequenceNumberEqual = null == accessCondition ? null : this.accessCondition.IfSequenceNumberEqual;
            ifSequenceNumberLessThan = null == accessCondition ? null : this.accessCondition.IfSequenceNumberLessThan;
            ifSequenceNumberLessThanOrEqual = null == accessCondition ? null : this.accessCondition.IfSequenceNumberLessThanOrEqual;
            leaseId = null == accessCondition ? null : this.accessCondition.LeaseId;
        }

        /// <summary>
        /// Initializes a deserialized AccessCondition
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            if (!string.IsNullOrEmpty(ifMatchETag)
                || null != ifModifiedSinceTime
                || !string.IsNullOrEmpty(ifNoneMatchETag)
                || null != ifNotModifiedSinceTime
                || null != ifSequenceNumberEqual
                || null != ifSequenceNumberLessThan
                || null != ifSequenceNumberLessThanOrEqual
                || !string.IsNullOrEmpty(leaseId))
            {
                this.accessCondition = new AccessCondition()
                {
                    IfMatchETag = ifMatchETag,
                    IfModifiedSinceTime = ifModifiedSinceTime,
                    IfNoneMatchETag = ifNoneMatchETag,
                    IfNotModifiedSinceTime = ifNotModifiedSinceTime,
                    IfSequenceNumberEqual = ifSequenceNumberEqual,
                    IfSequenceNumberLessThan = ifSequenceNumberLessThan,
                    IfSequenceNumberLessThanOrEqual = ifSequenceNumberLessThanOrEqual,
                    LeaseId = leaseId
                };
            }
            else
            {
                this.accessCondition = null;
            }
        }
#endif // !BINARY_SERIALIZATION
#endregion // Serialization helpers

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableAccessCondition"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableAccessCondition(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            string ifMatchETag = info.GetString(IfMatchETagName);
            DateTimeOffset? ifModifiedSinceTime = (DateTimeOffset?)info.GetValue(IfModifiedSinceTimeName, typeof(DateTimeOffset?));
            string ifNoneMatchETag = info.GetString(IfNoneMatchETagName);
            DateTimeOffset? ifNotModifiedSinceTime = (DateTimeOffset?)info.GetValue(IfNotModifiedSinceTimeName, typeof(DateTimeOffset?));
            long? ifSequenceNumberEqual = (long?)info.GetValue(IfSequenceNumberEqualName, typeof(long?));
            long? ifSequenceNumberLessThan = (long?)info.GetValue(IfSequenceNumberLessThanName, typeof(long?));
            long? ifSequenceNumberLessThanOrEqual = (long?)info.GetValue(IfSequenceNumberLessThanOrEqualName, typeof(long?));
            string leaseId = info.GetString(LeaseIdName);

            if (!string.IsNullOrEmpty(ifMatchETag)
                || null != ifModifiedSinceTime
                || !string.IsNullOrEmpty(ifNoneMatchETag)
                || null != ifNotModifiedSinceTime
                || null != ifSequenceNumberEqual
                || null != ifSequenceNumberLessThan
                || null != ifSequenceNumberLessThanOrEqual
                || !string.IsNullOrEmpty(leaseId))
            {
                this.accessCondition = new AccessCondition()
                {
                    IfMatchETag = ifMatchETag,
                    IfModifiedSinceTime = ifModifiedSinceTime,
                    IfNoneMatchETag = ifNoneMatchETag,
                    IfNotModifiedSinceTime = ifNotModifiedSinceTime,
                    IfSequenceNumberEqual = ifSequenceNumberEqual,
                    IfSequenceNumberLessThan = ifSequenceNumberLessThan,
                    IfSequenceNumberLessThanOrEqual = ifSequenceNumberLessThanOrEqual,
                    LeaseId = leaseId
                };
            }
            else 
            {
                this.accessCondition = null;
            }
        }
#endif // BINARY_SERIALIZATION

        internal AccessCondition AccessCondition
        {
            get
            {
                return this.accessCondition;
            }

            set
            {
                this.accessCondition = value;
            }
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
                throw new ArgumentNullException("info");
            }

            if (null == this.accessCondition)
            {
                info.AddValue(IfMatchETagName, null);
                info.AddValue(IfModifiedSinceTimeName, null);
                info.AddValue(IfNoneMatchETagName, null);
                info.AddValue(IfNotModifiedSinceTimeName, null);
                info.AddValue(IfSequenceNumberEqualName, null);
                info.AddValue(IfSequenceNumberLessThanName, null);
                info.AddValue(IfSequenceNumberLessThanOrEqualName, null);
                info.AddValue(LeaseIdName, null);
            }
            else
            {

                info.AddValue(IfMatchETagName, this.accessCondition.IfMatchETag);
                info.AddValue(IfModifiedSinceTimeName, this.accessCondition.IfModifiedSinceTime);
                info.AddValue(IfNoneMatchETagName, this.accessCondition.IfNoneMatchETag);
                info.AddValue(IfNotModifiedSinceTimeName, this.accessCondition.IfNotModifiedSinceTime);
                info.AddValue(IfSequenceNumberEqualName, this.accessCondition.IfSequenceNumberEqual);
                info.AddValue(IfSequenceNumberLessThanName, this.accessCondition.IfSequenceNumberLessThan);
                info.AddValue(IfSequenceNumberLessThanOrEqualName, this.accessCondition.IfSequenceNumberLessThanOrEqual);
                info.AddValue(LeaseIdName, this.accessCondition.LeaseId);
            }
        }
#endif // BINARY_SERIALIZATION

        internal static AccessCondition GetAccessCondition(SerializableAccessCondition serialization)
        {
            if (null == serialization)
            {
                return null;
            }

            return serialization.AccessCondition;
        }

        internal static void SetAccessCondition(
            ref SerializableAccessCondition serialization, 
            AccessCondition value)
        {            
            if ((null == serialization)
                && (null == value))
            {
                return;
            }

            if (null != serialization)
            {
                serialization.AccessCondition = value;
            }
            else
            {
                serialization = new SerializableAccessCondition()
                {
                    AccessCondition = value
                };
            }
        }
    }
}
