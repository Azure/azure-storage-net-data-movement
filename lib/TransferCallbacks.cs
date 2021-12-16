//------------------------------------------------------------------------------
// <copyright file="TransferCallbacks.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
	using System.Threading.Tasks;

    /// <summary>
    /// Callback invoked to tell whether to overwrite an existing destination.
    /// </summary>
    /// <param name="source">Instance of source used to overwrite the destination.</param>
    /// <param name="destination">Instance of destination to be overwritten.</param>
    /// <returns>True if the file should be overwritten; otherwise false.</returns>
    public delegate Task<bool> ShouldOverwriteCallbackAsync(
        object source,
        object destination);

    /// <summary>
    /// Callback invoked to tell whether a transfer should be done.
    /// </summary>
    /// <param name="source">Instance of the transfer source.</param>
    /// <param name="destination">Instance of the transfer destination.</param>
    /// <returns>True if the transfer should be done; otherwise false.</returns>
    public delegate Task<bool> ShouldTransferCallbackAsync(
        object source,
        object destination);

    /// <summary>
    /// Callback invoked to set destination's attributes in memory. 
    /// The attributes set in this callback will be sent to azure storage service. 
    /// </summary>
    /// <param name="source">Source instance in the transfer.</param>
    /// <param name="destination">Instance of destination to be overwritten.</param>
    public delegate Task SetAttributesCallbackAsync(object source, object destination);

    /// <summary>
    /// Callback invoked to tell whether a path to be transferred is valid.
    /// </summary>
    /// <param name="destination">Instance of the transfer destination.</param>
    public delegate Task ValidatePathCallbackAsync(object destination);
}
