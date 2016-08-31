//------------------------------------------------------------------------------
// <copyright file="GlobalMemoryStatusNativeMethods.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;

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
