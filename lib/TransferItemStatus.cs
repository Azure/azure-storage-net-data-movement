//-----------------------------------------------------------------------------
// <copyright file="TransferEventArgs.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
	/// <summary>
	/// Status of how the item was written to destination
	/// </summary>
	public enum TransferItemStatus
	{
		/// <summary>
		/// The item was created as a new one
		/// </summary>
		Created = 1,

		/// <summary>
		/// The item was overwritten
		/// </summary>
		Overwritten
	}
}