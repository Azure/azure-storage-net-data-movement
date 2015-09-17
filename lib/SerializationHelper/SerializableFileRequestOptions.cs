//------------------------------------------------------------------------------
// <copyright file="SerializableFileRequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Define class to serialize FileRequestOptions instance.
    /// </summary>
    [Serializable]
    internal sealed class SerializableFileRequestOptions : SerializableRequestOptions, ISerializable
    {
        private const string DisableContentMD5ValidationName = "DisableContentMD5Validation";
        private const string MaximumExecutionTimeName = "MaximumExecutionTime";
        private const string ServerTimeoutName = "ServerTimeout";
        private const string StoreFileContentMD5Name = "StoreFileContentMD5";
        private const string UseTransactionalMD5Name = "UseTransactionalMD5";

        private FileRequestOptions fileRequestOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableFileRequestOptions"/> class.
        /// </summary>
        public SerializableFileRequestOptions()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableFileRequestOptions"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        private SerializableFileRequestOptions(SerializationInfo info, StreamingContext context)
            : base (info, context)
        {
            bool? disableContentMD5Validation = (bool?)info.GetValue(DisableContentMD5ValidationName, typeof(bool?));
            TimeSpan? maximumExecutionTime = (TimeSpan?)info.GetValue(MaximumExecutionTimeName, typeof(TimeSpan?));
            TimeSpan? serverTimeout = (TimeSpan?)info.GetValue(ServerTimeoutName, typeof(TimeSpan?));
            bool? storeFileContentMD5 = (bool?)info.GetValue(StoreFileContentMD5Name, typeof(bool?));
            bool? useTransactionalMD5 = (bool?)info.GetValue(UseTransactionalMD5Name, typeof(bool?));

            if (null != disableContentMD5Validation
                || null != maximumExecutionTime
                || null != serverTimeout
                || null != storeFileContentMD5
                || null != useTransactionalMD5)
            {
                this.fileRequestOptions = Transfer_RequestOptions.DefaultFileRequestOptions;

                this.fileRequestOptions.DisableContentMD5Validation = disableContentMD5Validation;
                this.fileRequestOptions.MaximumExecutionTime = maximumExecutionTime;
                this.fileRequestOptions.ServerTimeout = serverTimeout;
                this.fileRequestOptions.StoreFileContentMD5 = storeFileContentMD5;
                this.fileRequestOptions.UseTransactionalMD5 = useTransactionalMD5;
            }
            else
            {
                this.fileRequestOptions = null;
            }
        }

        protected override IRequestOptions RequestOptions
        {
            get
            {
                return this.fileRequestOptions;
            }

            set
            {
                FileRequestOptions requestOptions = value as FileRequestOptions;
                Debug.Assert(null != requestOptions, "Setting RequestOptions in FlobRequestOptionsSerializer, but the value is not a FileRequestOptions instance.");

                this.fileRequestOptions = requestOptions;
            }
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (null == this.fileRequestOptions)
            {
                info.AddValue(DisableContentMD5ValidationName, null);
                info.AddValue(MaximumExecutionTimeName, null, typeof(TimeSpan?));
                info.AddValue(ServerTimeoutName, null, typeof(TimeSpan?));
                info.AddValue(StoreFileContentMD5Name, null);
                info.AddValue(UseTransactionalMD5Name, null);
            }
            else
            {
                info.AddValue(DisableContentMD5ValidationName, this.fileRequestOptions.DisableContentMD5Validation);
                info.AddValue(MaximumExecutionTimeName, this.fileRequestOptions.MaximumExecutionTime, typeof(TimeSpan?));
                info.AddValue(ServerTimeoutName, this.fileRequestOptions.ServerTimeout, typeof(TimeSpan?));
                info.AddValue(StoreFileContentMD5Name, this.fileRequestOptions.StoreFileContentMD5);
                info.AddValue(UseTransactionalMD5Name, this.fileRequestOptions.UseTransactionalMD5);
            }
        }
    }
}
