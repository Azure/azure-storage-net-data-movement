//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.Runtime.InteropServices;

    internal static partial class NativeMethods
    {
        const int HOST_VM_INFO = 2;

        #region P/Invokes
        [DllImport("libc", CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        internal static extern int sysctlbyname([MarshalAs(UnmanagedType.LPTStr)]string name, ref int oldp, ref int oldlenp, IntPtr newp, int newlen);
        
        [DllImport("libc")]
        private static extern IntPtr mach_host_self();

        [DllImport("libc")]
        private static extern IntPtr host_statistics(IntPtr host, int hostFlavor, ref vm_statistics vmStat, ref int count);
        #endregion // P/Invokes

        #region Helper methods
        internal static vm_statistics GetOSXHostStatistics()
        {
            var mach_host = mach_host_self();
#if GENERIC_MARSHAL_SIZEOF
            var statisticsInfoCount = Marshal.SizeOf<vm_statistics>() / Marshal.SizeOf<int>();
#else // GENERIC_MARSHAL_SIZEOF
            var statisticsInfoCount = Marshal.SizeOf(typeof(vm_statistics)) / Marshal.SizeOf(typeof(int));
#endif // GENERIC_MARSHAL_SIZEOF
            vm_statistics vmStats = new vm_statistics();
            host_statistics(mach_host, HOST_VM_INFO, ref vmStats, ref statisticsInfoCount);
            return vmStats;
        }
        #endregion // Helper methods

        #region Helper structs
        internal struct vm_statistics
        {
            public int free_count;
            public int active_count;
            public int inactive_count;
            public int wire_count;
            public int zero_fill_count;
            public int reactivations;
            public int pageins;
            public int pageouts;
            public int faults;
            public int cow_faults;
            public int lookups;
            public int hits;
        }
        #endregion // Helper structs
    }
}