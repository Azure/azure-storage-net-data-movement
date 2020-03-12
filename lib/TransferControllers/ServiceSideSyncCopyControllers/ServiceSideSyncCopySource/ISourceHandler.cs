//------------------------------------------------------------------------------
// <copyright file="FileSourceHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopySource
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///  Service side sync copy can copy between any two ends of append blob, block blob, page blob, and cloud file,
    ///  which is a matrix with 16 directions. 
    ///  All copying shares similar logic with minor differences. 
    ///  Use ISourceHandler and IDestHandler to deal with minor differences, and let main controller deal with the whole copying logic.
    /// </summary>
    internal interface ISourceHandler
    {
        Task FetchAttributesAsync(CancellationToken cancellationToken);

        Task DownloadRangeToStreamAsync(Stream stream, 
            long startOffset, 
            long length, 
            AccessCondition accessCondition,
            bool useTransactionalMD5,
            OperationContext operationContext, 
            CancellationToken cancellationToken);

        Uri GetCopySourceUri();

        Uri Uri { get; }

        Attributes SourceAttributes { get; }
        long TotalLength { get; }

        string ETag { get; }

        AccessCondition AccessCondition { get; }

        bool NeedToCheckAccessCondition { get; }
    }
}
