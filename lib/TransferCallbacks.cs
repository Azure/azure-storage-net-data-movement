//------------------------------------------------------------------------------
// <copyright file="TransferCallbacks.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    /// <summary>
    /// Callback invoked to tell whether to overwrite an existing destination.
    /// </summary>
    /// <param name="source">Instance of source used to overwrite the destination.</param>
    /// <param name="destination">Instance of destination to be overwritten.</param>
    /// <returns>True if the file should be overwritten; otherwise false.</returns>
    public delegate bool ShouldOverwriteCallback(
        object source,
        object destination);

    /// <summary>
    /// Callback invoked to tell whether a transfer should be done.
    /// </summary>
    /// <param name="source">Instance of the transfer source.</param>
    /// <param name="destination">Instance of the transfer destination.</param>
    /// <returns>True if the transfer should be done; otherwise false.</returns>
    public delegate bool ShouldTransferCallback(
        object source,
        object destination);

    /// <summary>
    /// Callback invoked to set destination's attributes in memory. 
    /// The attributes set in this callback will be sent to azure storage service. 
    /// </summary>
    /// <param name="destination">Instance of destination to be overwritten.</param>
    public delegate void SetAttributesCallback(object destination);
}
