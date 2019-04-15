//------------------------------------------------------------------------------
// <copyright file="AzureBlobToAzureBlobNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System.Diagnostics;

    /// <summary>
    /// Name resolver class for translating Azure blob names to Azure blob names.
    /// </summary>
    internal class AzureBlobToAzureBlobNameResolver : INameResolver
    {
        public string ResolveName(TransferEntry sourceEntry)
        {
            AzureBlobEntry blobEntry = sourceEntry as AzureBlobEntry;
            Debug.Assert(blobEntry != null, "blobEntry");

            return Utils.AppendSnapShotTimeToFileName(sourceEntry.RelativePath, blobEntry.Blob.SnapshotTime);
        }
    }
}
