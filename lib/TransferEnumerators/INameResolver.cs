//------------------------------------------------------------------------------
// <copyright file="INameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// INameResolver interface.
    /// </summary>
    internal interface INameResolver
    {
        string ResolveName(TransferEntry sourceEntry);
    }
}
