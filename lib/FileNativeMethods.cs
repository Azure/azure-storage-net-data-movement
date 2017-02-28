using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
// using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    internal class FileNativeMethods
    {
        private FileNativeMethods()
        {
        }

        [System.Runtime.InteropServices.StructLayout(LayoutKind.Sequential)]
        public struct OFSTRUCT
        {
            public byte cBytes;
            public byte fFixedDisc;
            public UInt16 nErrCode;
            public UInt16 Reserved1;
            public UInt16 Reserved2;
            [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValTStr, SizeConst = 128)]
            public string szPathName;
        }

        // Doesn't support Unicode file name.
        // [DllImport("kernel32.dll", BestFitMapping = false, ThrowOnUnmappableChar = true)]
        // static extern int OpenFile([System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.LPStr)]string lpFileName, out OFSTRUCT lpReOpenBuff, UInt32 uStyle);

        // Open or create file
        /*[DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern IntPtr CreateFileW(
             [MarshalAs(UnmanagedType.LPWStr)] string filename,
             [MarshalAs(UnmanagedType.U4)] FileAccess access,
             [MarshalAs(UnmanagedType.U4)] FileShare share,
             IntPtr securityAttributes,
             [MarshalAs(UnmanagedType.U4)] FileMode creationDisposition,
             [MarshalAs(UnmanagedType.U4)] FileAttributes flagsAndAttributes,
             IntPtr templateFile);
        */

        // Open or create file
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern SafeFileHandle CreateFile(
             [MarshalAs(UnmanagedType.LPWStr)] string filename,
             [MarshalAs(UnmanagedType.U4)] FileAccess access,
             [MarshalAs(UnmanagedType.U4)] FileShare share,
             IntPtr securityAttributes,
             [MarshalAs(UnmanagedType.U4)] FileMode creationDisposition,
             [MarshalAs(UnmanagedType.U4)] FileAttributes flagsAndAttributes,
             IntPtr templateFile);

#if !DOTNET5_4
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool ReadFile(SafeFileHandle hFile, [Out] byte[] lpBuffer, uint nNumberOfBytesToRead, out uint lpNumberOfBytesRead, ref NativeOverlapped template);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool WriteFile(SafeFileHandle hFile, byte[] lpBuffer, uint nNumberOfBytesToWrite, out uint lpNumberOfBytesWritten, ref NativeOverlapped template);
#else

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool ReadFile(SafeFileHandle hFile, [Out] byte[] lpBuffer, uint nNumberOfBytesToRead, out uint lpNumberOfBytesRead, IntPtr template);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool WriteFile(SafeFileHandle hFile, byte[] lpBuffer, uint nNumberOfBytesToWrite, out uint lpNumberOfBytesWritten, IntPtr template);
#endif
        // [DllImport("kernel32.dll", SetLastError = true)]
        // [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        // [SuppressUnmanagedCodeSecurity]
        // [return: MarshalAs(UnmanagedType.Bool)]
        // public static extern bool CloseHandle(SafeFileHandle hObject);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool SetEndOfFile(SafeFileHandle hFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint GetFileSize(SafeFileHandle hFile, IntPtr lpFileSizeHigh);

        [DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern int SetFilePointer(SafeFileHandle handle, int lDistanceToMove, out int lpDistanceToMoveHigh, uint dwMoveMethod);

        [DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.U4)]
        public static extern uint GetFullPathNameW(
            string lpFileName,
            uint nBufferLength,
            [Out] StringBuilder lpBuffer,
            [Out] StringBuilder lpFilePart);

        public static long Seek(SafeFileHandle handle, long offset, SeekOrigin origin)
        {
            uint moveMethod = 0;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    moveMethod = 0;
                    break;

                case SeekOrigin.Current:
                    moveMethod = 1;
                    break;

                case SeekOrigin.End:
                    moveMethod = 2;
                    break;
            }

            int lo = (int)(offset & 0xffffffff);
            int hi = (int)(offset >> 32);

            lo = SetFilePointer(handle, lo, out hi, moveMethod);

            if (lo == -1)
            {
                Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            return (((long)hi << 32) | (uint)lo);
        }
    }
}
