//------------------------------------------------------------------------------
// <copyright file="TransferException.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base exception class for exceptions thrown by Blob/FileTransferJobs.
    /// </summary>
#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    public class TransferException : Exception
    {
        /// <summary>
        /// Version of current TransferException serialization format.
        /// </summary>
        private const int ExceptionVersion = 1;

        /// <summary>
        /// Serialization field name for Version.
        /// </summary>
        private const string VersionFieldName = "Version";

        /// <summary>
        /// Serialization field name for ErrorCode.
        /// </summary>
        private const string ErrorCodeFieldName = "ErrorCode";

        /// <summary>
        /// Transfer error code.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        private TransferErrorCode errorCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        public TransferException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TransferException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="ex">The exception that is the cause of the current exception, or a null reference
        /// if no inner exception is specified.</param>
        public TransferException(string message, Exception ex)
            : base(message, ex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        public TransferException(TransferErrorCode errorCode)
        {
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        /// <param name="message">Exception message.</param>
        public TransferException(
            TransferErrorCode errorCode, 
            string message)
            : base(message)
        {
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        /// <param name="message">Exception message.</param>
        /// <param name="innerException">Inner exception.</param>
        public TransferException(
            TransferErrorCode errorCode, 
            string message, 
            Exception innerException)
            : base(message, innerException)
        {
            this.errorCode = errorCode;
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        protected TransferException(
            SerializationInfo info, 
            StreamingContext context)
            : base(info, context)
        {
            int exceptionVersion = info.GetInt32(VersionFieldName);

            if (exceptionVersion >= 1)
            {
                this.errorCode = (TransferErrorCode)info.GetInt32(ErrorCodeFieldName);
            }
        }
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Gets the detailed error code.
        /// </summary>
        /// <value>The error code of the exception.</value>
        public TransferErrorCode ErrorCode
        {
            get
            {
                return this.errorCode;
            }
        }

#if BINARY_SERIALIZATION
        /// <summary>
        /// Serializes the exception.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public override void GetObjectData(
            SerializationInfo info, 
            StreamingContext context)
        {
            if (null == info)
            {
                throw new ArgumentNullException("info");
            }

            info.AddValue(VersionFieldName, ExceptionVersion);
            info.AddValue(ErrorCodeFieldName, this.errorCode);

            base.GetObjectData(info, context);
        }
#endif //BINARY_SERIALIZATION
    }
}
