//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Defines native helper methods.
    /// </summary>
    internal static class CrossPlatformHelpers
    {
        public static bool IsWindows
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#else
                return true;
#endif // RUNTIME_INFORMATION
            }
        }

        public static bool IsOSX
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
#else
                return false;
#endif // RUNTIME_INFORMATION
            }
        }

        public static bool IsLinux
        {
            get
            {
#if RUNTIME_INFORMATION
                return RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
#else
                return false;
#endif // RUNTIME_INFORMATION
            }
        }

        /// <summary>
        /// Retrieve the available memory (in bytes) as appropriate
        /// for the current platform
        /// </summary>
        /// <returns>Bytes of available memory</returns>
        public static ulong GetAvailableMemory()
        {
            if (IsWindows)
            {
                try
                {
                    var memStatus = new NativeMethods.MEMORYSTATUSEX();
                    if (NativeMethods.GlobalMemoryStatusEx(memStatus))
                    {
                        return memStatus.ullAvailPhys;
                    }
                }
                catch (TypeLoadException)
                {
                    // GlobalMemoryStatusEx exists in all Windows editions DMLib is supported on,
                    // but watch for the case where it doesn't out of an abundance of caution
                    return 0;
                }
            }

            if (IsLinux)
            {
                var memInfo = NativeMethods.GetLinuxMemoryInfo();

                if (memInfo.ContainsKey("MemAvailable") && memInfo["MemAvailable"] != 0)
                {
                    // Newer Linux builds will include a 'MemAvailable' statistic which contains
                    // the necessary information
                    return memInfo["MemAvailable"] * 1024; // MemInfo values are stored as kB
                }
                else if (memInfo.ContainsKey("MemFree") && memInfo.ContainsKey("Cached"))
                {
                    // If MemAvailable is not available, it can be approximated by summing
                    // MemFree and Cached
                    return (memInfo["MemFree"] + memInfo["Cached"]) * 1024; // MemInfo values are stored as kB
                }
                else
                {
                    return 0;
                }
            }

            if (IsOSX)
            {
                try
                {
                    // Get page size
                    var pageSize = 4096; // Default page size to 4096 if sysctl fails
#if GENERIC_MARSHAL_SIZEOF
                    var pageSizeSize = Marshal.SizeOf<int>();
#else // GENERIC_MARSHAL_SIZEOF
                var pageSizeSize = Marshal.SizeOf(typeof(int));
#endif // GENERIC_MARSHAL_SIZEOF
                    if (0 != NativeMethods.sysctlbyname("hw.pagesize", ref pageSize, ref pageSizeSize, IntPtr.Zero, 0))
                    {
                        // May need to get error message here.
                        return 0;
                    }

                    // Get number of free pages
                    var freePages = 0;
#if GENERIC_MARSHAL_SIZEOF
                    var freePagesSize = Marshal.SizeOf<int>();
#else // GENERIC_MARSHAL_SIZEOF
                var freePagesSize = Marshal.SizeOf(typeof(int));
#endif // GENERIC_MARSHAL_SIZEOF
                    if (0 != NativeMethods.sysctlbyname("vm.page_free_count", ref freePages, ref freePagesSize, IntPtr.Zero, 0))
                    {
                        return 0;
                    }

                    if (freePages == 0)
                    {
                        // If sysctl failed to retrieve the number of free pages, try with host_statistics
                        var vmStats = NativeMethods.GetOSXHostStatistics();
                        freePages = vmStats.free_count;
                    }

                    return ((ulong)freePages) * ((ulong)pageSize);
                }
                catch (TypeLoadException)
                {
                    // In case this is somehow run on an OSX platform without sysctl or host statistics APIs exposed
                    return 0;
                }
            }

            // If the platform is unrecognized, default to returning 0
            return 0;

        }
    }
}