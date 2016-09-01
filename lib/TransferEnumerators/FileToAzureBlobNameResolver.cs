//------------------------------------------------------------------------------
// <copyright file="FileToAzureNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    /// <summary>
    /// Name resolver class for translating Windows file names to Azure blob/file names.
    /// </summary>
    internal class FileToAzureBlobNameResolver : INameResolver
    {
        public string ResolveName(TransferEntry sourceEntry)
        {
            return sourceEntry.RelativePath.Replace('\\', '/');
        }
    }
}
