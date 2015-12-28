//------------------------------------------------------------------------------
// <copyright file="SerializableRequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;

    [Serializable]
    internal abstract class SerializableRequestOptions : ISerializable
    {
        protected SerializableRequestOptions()
        { 
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableRequestOptions"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected SerializableRequestOptions(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }
        }

        protected abstract IRequestOptions RequestOptions
        {
            get;
            set;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }
        }

        internal static IRequestOptions GetRequestOptions(SerializableRequestOptions serializer)
        {
            if (null == serializer)
            {
                return null;
            }

            return serializer.RequestOptions;
        }

        internal static void SetRequestOptions(ref SerializableRequestOptions serializer, IRequestOptions requestOptions)
        {
            if (null == serializer && null == requestOptions)
            {
                return;
            }

            if (null == serializer)
            {
                serializer = CreateSerializableRequestOptions(requestOptions);
            }
            else
            {
                if ((requestOptions is FileRequestOptions)
                    && (serializer is SerializableBlobRequestOptions))
                {
                    serializer = new SerializableFileRequestOptions();
                }
                else if ((requestOptions is BlobRequestOptions)
                    && (serializer is SerializableFileRequestOptions))
                {
                    serializer = new SerializableBlobRequestOptions();
                }

                serializer.RequestOptions = requestOptions;
            }
        }

        private static SerializableRequestOptions CreateSerializableRequestOptions(IRequestOptions requestOptions)
        {
            if (requestOptions is FileRequestOptions)
            {
                return new SerializableFileRequestOptions()
                {
                    RequestOptions = requestOptions
                };
            }
            else 
            {
                Debug.Assert(requestOptions is BlobRequestOptions, "Request options should be an instance of BlobRequestOptions when code reach here.");
                return new SerializableBlobRequestOptions()
                {
                    RequestOptions = requestOptions
                };
            }
        }
    }
}
