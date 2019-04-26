//------------------------------------------------------------------------------
// <copyright file="FileNativeMethodsExtension.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Win32.SafeHandles;

    internal static partial class NativeMethods
    {
        [DllImport("kernel32.dll", EntryPoint = "RemoveDirectoryW", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool RemoveDirectoryW([MarshalAs(UnmanagedType.LPWStr)]string lpPathName);

        [DllImport("kernel32.dll", EntryPoint = "DeleteFileW", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool DeleteFileW([MarshalAs(UnmanagedType.LPWStr)]string lpFileName);
    }
}
