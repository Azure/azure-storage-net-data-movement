//------------------------------------------------------------------------------
// <copyright file="OverwriteCallback.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    /// <summary>
    /// Callback invoked to tell whether to overwrite an existing destination
    /// </summary>
    /// <param name="sourcePath">Path of the source file used to overwrite the destination.</param>
    /// <param name="destinationPath">Path of the file to be overwritten.</param>
    /// <returns>True if the file should be overwritten; otherwise false.</returns>
    public delegate bool OverwriteCallback(
        string sourcePath,
        string destinationPath);
}
