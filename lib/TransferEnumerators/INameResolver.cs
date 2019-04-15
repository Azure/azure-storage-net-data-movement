//------------------------------------------------------------------------------
// <copyright file="INameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// INameResolver interface.
    /// </summary>
    internal interface INameResolver
    {
        string ResolveName(TransferEntry sourceEntry);
    }
}
