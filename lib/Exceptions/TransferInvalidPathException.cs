//------------------------------------------------------------------------------
// <copyright file="TransferInvalidPathException.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
	using System;
	using System.Runtime.Serialization;

	/// <summary>
	/// Exceptions thrown when transfer skips.
	/// </summary>
#if BINARY_SERIALIZATION
	[Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
	public class TransferInvalidPathException : TransferException
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		public TransferInvalidPathException()
			: base(TransferErrorCode.FailToValidateSource)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		/// <param name="errorMessage">The message that describes the error.</param>
		public TransferInvalidPathException(string errorMessage)
			: base(TransferErrorCode.FailToValidateSource, errorMessage)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		/// <param name="errorMessage">Exception message.</param>
		/// <param name="innerException">Inner exception.</param>
		public TransferInvalidPathException(string errorMessage, Exception innerException)
			: base(TransferErrorCode.FailToValidateSource, errorMessage, innerException)
		{
		}

#if BINARY_SERIALIZATION
		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		/// <param name="info">Serialization information.</param>
		/// <param name="context">Streaming context.</param>
		protected TransferInvalidPathException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
#endif // BINARY_SERIALIZATION
	}
}