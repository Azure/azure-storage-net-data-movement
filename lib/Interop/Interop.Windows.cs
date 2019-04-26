//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Security;
    using Microsoft.Win32.SafeHandles;

    internal static partial class NativeMethods
    {
#if PINVOKE_TO_API_SETS
        private const string CORE_FILE_APIS = "api-ms-win-core-file-l1-2-1.dll";
        private const string CORE_SYSINFO_APIS = "api-ms-win-core-sysinfo-l1-2-1.dll";
#else // PINVOKE_TO_API_SETS
        private const string CORE_FILE_APIS = "kernel32.dll";
        private const string CORE_SYSINFO_APIS = "kernel32.dll";
#endif // PINVOKE_TO_API_SETS

        #region P/Invokes
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport(CORE_SYSINFO_APIS, CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        [DllImport(CORE_FILE_APIS, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FindClose(SafeHandle findFileHandle);

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode), BestFitMapping(false)]
        public struct WIN32_FIND_DATA
        {
            public FileAttributes FileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
            public uint FileSizeHigh;
            public uint FileSizeLow;
            public int Reserved0;
            public int Reserved1;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
            public string FileName;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 14)]
            public string AlternateFileName;
        }

#if SAFE_FILE_HANDLE_ZERO_OR_MINUS_ON_IS_INVALID
        public sealed class SafeFindHandle : SafeHandleZeroOrMinusOneIsInvalid
        {
            [SecurityCritical]
            internal SafeFindHandle()
                : base(true)
            {
            }
#else
        public sealed class SafeFindHandle : SafeHandle
        {
            [SecurityCritical]
            internal SafeFindHandle()
                : base(IntPtr.Zero, true)
            {
            }

            public override bool IsInvalid
            {
                get { return handle == IntPtr.Zero || handle == (IntPtr)(-1); }
            }
#endif
            protected override bool ReleaseHandle()
            {
                if (!(this.IsInvalid || this.IsClosed))
                {
                    return NativeMethods.FindClose(this);
                }

                return this.IsInvalid || this.IsClosed;
            }

            protected override void Dispose(bool disposing)
            {
                if (!(this.IsInvalid || this.IsClosed))
                {
                    NativeMethods.FindClose(this);
                }

                base.Dispose(disposing);
            }
        }
        #endregion // P/Invokes

        #region Helper structs
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal class MEMORYSTATUSEX
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
#if GENERIC_MARSHAL_SIZEOF
                this.dwLength = (uint)Marshal.SizeOf<MEMORYSTATUSEX>();
#else // GENERIC_MARSHAL_SIZEOF
            this.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
#endif // GENERIC_MARSHAL_SIZEOF
            }
        }
        #endregion // Helper structs
    }
}