namespace Microsoft.Azure.Storage.DataMovement
{
	using System;
	using System.Runtime.Serialization;

	/// <summary>
	/// Exceptions thrown when transfer stucks.
	/// </summary>
#if BINARY_SERIALIZATION
	[Serializable]
#else
	[DataContract]
#endif // BINARY_SERIALIZATION
	public class TransferStuckException : TransferException
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="TransferStuckException" /> class.
		/// </summary>
		public TransferStuckException()
			: base(TransferErrorCode.TransferStuck)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferStuckException" /> class.
		/// </summary>
		/// <param name="errorMessage">The message that describes the error.</param>
		public TransferStuckException(string errorMessage)
			: base(TransferErrorCode.TransferStuck, errorMessage)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferStuckException" /> class.
		/// </summary>
		/// <param name="errorMessage">Exception message.</param>
		/// <param name="innerException">Inner exception.</param>
		public TransferStuckException(string errorMessage, Exception innerException)
			: base(TransferErrorCode.TransferStuck, errorMessage, innerException)
		{
		}

#if BINARY_SERIALIZATION
		/// <summary>
		/// Initializes a new instance of the <see cref="TransferStuckException" /> class.
		/// </summary>
		/// <param name="info">Serialization information.</param>
		/// <param name="context">Streaming context.</param>
		protected TransferStuckException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
#endif // BINARY_SERIALIZATION
	}
}