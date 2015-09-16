//------------------------------------------------------------------------------
// <copyright file="TransferLocationType.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    internal enum TransferLocationType
    {
        FilePath,
        Stream,
        AzureBlob,
        AzureFile,
        SourceUri
    }
}
