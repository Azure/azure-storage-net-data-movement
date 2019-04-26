//------------------------------------------------------------------------------
// <copyright file="TransferEnumeratorBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// Base class of dmlib internal transfer enumerators. It contains the common properties of transfer enumerators.
    /// </summary>
    internal abstract class TransferEnumeratorBase
    {
        public string SearchPattern
        {
            get;
            set;
        }

        public bool Recursive
        {
            get;
            set;
        }
    }
}
