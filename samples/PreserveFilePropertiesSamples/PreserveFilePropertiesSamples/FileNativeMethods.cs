//------------------------------------------------------------------------------
// <copyright file="FileNativeMethods.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace PreserveFilePropertiesSamples
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using Microsoft.Win32.SafeHandles;

    static class FileNativeMethods
    {
#if !DOTNET5_4
        public const int ERROR_SUCCESS = 0;
        public const int ERROR_FILE_NOT_FOUND = 2;
        public const int ERROR_DIRECTORY_NOT_FOUND = 3;
        public const int ERROR_NO_MORE_FILES = 18;
        public const int ERROR_ALREADY_EXISTS = 183;
        public const int ERROR_HANDLE_EOF = 38;
        public const uint FILE_BEGIN = 0;
        public const uint INVALID_SET_FILE_POINTER = 0xFFFFFFFF;

        public const uint GENERIC_READ = 0x80000000;
        public const uint GENERIC_WRITE = 0x40000000;
        public const uint GENERIC_READ_WRITE = GENERIC_READ | GENERIC_WRITE;

        // This flag must be set to obtain a handle to a directory.
        public const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;

        // Open or create file
        [DllImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern SafeFileHandle GetFileHandleW(
             [MarshalAs(UnmanagedType.LPWStr)] string filename,
             [MarshalAs(UnmanagedType.U4)] uint access,
             [MarshalAs(UnmanagedType.U4)] FileShare share,
             IntPtr securityAttributes,
             [MarshalAs(UnmanagedType.U4)] FileMode creationDisposition,
             [MarshalAs(UnmanagedType.U4)] uint flagsAndAttributes,
             IntPtr templateFile);

        [DllImport("kernel32.dll", EntryPoint = "GetFileAttributesW", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern uint GetFileAttributesW(string lpFileName);

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2205:UseManagedEquivalentsOfWin32Api")]
        [DllImport("kernel32.dll", EntryPoint = "SetFileAttributesW", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool SetFileAttributesW(string lpFileName, uint dwFileAttributes);

        [StructLayout(LayoutKind.Sequential)]
        public struct FILETIME
        {
            public uint dwLowDateTime;
            public uint dwHighDateTime;
        };

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool SetFileTime(
            SafeFileHandle hFile,
            ref FILETIME lpCreationTime,
            ref FILETIME lpLastAccessTime,
            ref FILETIME lpLastWriteTime);

        /// <summary>
        /// Throw exception if last Win32 error is not zero.
        /// </summary>
        public static void ThrowExceptionForLastWin32ErrorIfExists()
        {
            ThrowExceptionForLastWin32ErrorIfExists(new int[] {
                ERROR_SUCCESS
            });
        }

        /// <summary>
        /// Throw exception if last Win32 error is not expected.
        /// </summary>
        /// <param name="expectErrorCodes">Error codes that are expected.</param>
        public static void ThrowExceptionForLastWin32ErrorIfExists(int[] expectErrorCodes)
        {
            int errorCode = Marshal.GetLastWin32Error();

            if (expectErrorCodes != null
                && expectErrorCodes.Contains(errorCode))
            {
                return;
            }
            throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
        }

        /// <summary>
        /// Throw exception if the Win32 error given is not expected.
        /// </summary>
        /// <param name="errorCode">Win32 error code want to check.</param>
        /// <param name="expectErrorCodes">Error codes that are expected.</param>
        public static void ThrowExceptionForLastWin32ErrorIfExists(int errorCode, int[] expectErrorCodes)
        {
            if (expectErrorCodes != null
                && expectErrorCodes.Contains(errorCode))
            {
                return;
            }
            throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
        }
#endif
    }
}
