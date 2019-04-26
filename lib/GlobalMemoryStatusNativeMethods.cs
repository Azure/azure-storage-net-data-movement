//------------------------------------------------------------------------------
// <copyright file="GlobalMemoryStatusNativeMethods.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using Microsoft.Azure.Storage.DataMovement.Interop;

    internal class GlobalMemoryStatusNativeMethods
    {
        public GlobalMemoryStatusNativeMethods()
        {
            this.AvailablePhysicalMemory = CrossPlatformHelpers.GetAvailableMemory();
        }
        
        public ulong AvailablePhysicalMemory
        {
            get;
            private set;
        }
    }
}
