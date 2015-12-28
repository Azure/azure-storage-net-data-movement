//------------------------------------------------------------------------------
// <copyright file="SerializableAccessCondition.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    internal sealed class SerializableAccessCondition : ISerializable
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
