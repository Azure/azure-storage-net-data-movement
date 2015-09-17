//------------------------------------------------------------------------------
// <copyright file="GlobalMemoryStatusNativeMethods.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System.Runtime.InteropServices;

    internal class GlobalMemoryStatusNativeMethods
    {
        private MEMORYSTATUSEX memStatus;

        public GlobalMemoryStatusNativeMethods()
        {
            this.memStatus = new MEMORYSTATUSEX();
            if (GlobalMemoryStatusEx(this.memStatus))
            {
                this.AvailablePhysicalMemory = this.memStatus.ullAvailPhys;
            }
        }

        public ulong AvailablePhysicalMemory
        {
            get;
            private set;
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        private class MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;

            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            }
        }
    }
}
