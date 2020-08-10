//------------------------------------------------------------------------------
// <copyright file="IDestHandler.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferControllers.ServiceSideSyncCopyDest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///  Service side sync copy can copy between any two ends of append blob, block blob, page blob, and cloud file,
    ///  which is a matrix with 16 directions. 
    ///  All copying shares similar logic with minor differences. 
    ///  Use SourceHandler and DestHandler to deal with minor differences, and let the main controller deal with the whole copying logic.
    /// </summary>
    internal interface IDestHandler
    {
        Uri Uri { get; }

        Task<bool> CheckAndCreateDestinationAsync(
            bool isForceOverwrite,
            long totalLength,
            Func<bool, Task> checkOverwrite,
            CancellationToken cancellationToken);

        Task CommitAsync(
            bool gotDestAttributes,
            Attributes sourceAttributes,
            Func<object, object, Task> setCustomAttributes,
            CancellationToken cancellationToken);
    }
}
