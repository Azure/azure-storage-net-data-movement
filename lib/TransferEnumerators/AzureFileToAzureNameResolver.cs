//------------------------------------------------------------------------------
// <copyright file="AzureFileToAzureNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// Name resolver for translating Azure file names to Azure blob or Azure file names.
    /// </summary>
    class AzureFileToAzureNameResolver : INameResolver
    {
        public string ResolveName(TransferEntry sourceEntry)
        {
            return sourceEntry.RelativePath.TrimEnd(new char[] {' '}).Replace('\\', '/');
        }
    }
}
