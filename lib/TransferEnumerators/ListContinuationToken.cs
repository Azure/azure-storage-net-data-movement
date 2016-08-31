//------------------------------------------------------------------------------
// <copyright file="ListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class of list continuation tokens
    /// </summary>
#if !BINARY_SERIALIZATION
    [DataContract]
#endif
    internal abstract class ListContinuationToken
    {
    }
}
