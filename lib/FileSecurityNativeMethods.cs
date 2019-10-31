//------------------------------------------------------------------------------
// <copyright file="FileSecurityNativeMethods.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;

    internal static partial class NativeMethods
    {
        public const int SE_PRIVILEGE_ENABLED = 0x00000002;
        public const int SE_PRIVILEGE_DISABLED = 0x00000000;

        public const int ERROR_ACCESS_DENIED = 5;
        public const int ERROR_NOT_ALL_ASSIGNED = 1300;
        public const int ERROR_INVALID_OWNER = 1307;
        public const int ERROR_PRIVILEGE_NOT_HELD = 1314;
        public const int ERROR_INSUFFICIENT_BUFFER = 122;
        public const int SDDL_REVISION_1 = 1;

        public const string SACLPrivilegeName = "SeSecurityPrivilege";
        public const string OwnerPrivilegeName = "SeRestorePrivilege";

        [Flags]
        public enum SECURITY_INFORMATION : uint
        {
            OWNER_SECURITY_INFORMATION = 0x00000001,
            GROUP_SECURITY_INFORMATION = 0x00000002,
            DACL_SECURITY_INFORMATION = 0x00000004,
            SACL_SECURITY_INFORMATION = 0x00000008,
            UNPROTECTED_SACL_SECURITY_INFORMATION = 0x10000000,
            UNPROTECTED_DACL_SECURITY_INFORMATION = 0x20000000,
            PROTECTED_SACL_SECURITY_INFORMATION = 0x40000000,
            PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
        }

        public enum SE_OBJECT_TYPE
        {
            SE_UNKNOWN_OBJECT_TYPE = 0,
            SE_FILE_OBJECT,
            SE_SERVICE,
            SE_PRINTER,
            SE_REGISTRY_KEY,
            SE_LMSHARE,
            SE_KERNEL_OBJECT,
            SE_WINDOW_OBJECT,
            SE_DS_OBJECT,
            SE_DS_OBJECT_ALL,
            SE_PROVIDER_DEFINED_OBJECT,
            SE_WMIGUID_OBJECT,
            SE_REGISTRY_WOW64_32KEY
        }

        public enum SID_NAME_USE
        {
            SidTypeUser,
            SidTypeGroup,
            SidTypeDomain,
            SidTypeAlias,
            SidTypeWellKnownGroup,
            SidTypeDeletedAccount,
            SidTypeInvalid,
            SidTypeUnknown,
            SidTypeComputer,
            SidTypeLabel,
            SidTypeLogonSession
        };

        [StructLayout(LayoutKind.Sequential)]
        public struct LUID
        {
            public UInt32 LowPart;
            public Int32 HighPart;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 4)]
        public struct LUID_AND_ATTRIBUTES
        {
            public LUID Luid;
            public UInt32 Attributes;
        }

        public struct TOKEN_PRIVILEGES
        {
            public int PrivilegeCount;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public LUID_AND_ATTRIBUTES[] Privileges;
        }

        #region Get/Set FileDDL
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2205:UseManagedEquivalentsOfWin32Api")]
        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetUserName(StringBuilder sb, ref Int32 length);

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool LookupAccountName(
            string lpSystemName,
            string lpAccountName,
            [MarshalAs(UnmanagedType.LPArray)] byte[] Sid,
            ref uint cbSid,
            StringBuilder ReferencedDomainName,
            ref uint cchReferencedDomainName,
            out SID_NAME_USE peUse);

        [DllImport("advapi32", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetWindowsAccountDomainSid(
            [MarshalAs(UnmanagedType.LPArray)] byte[] sid,
            [MarshalAs(UnmanagedType.LPArray)] byte[] domainSid,
            ref uint length);

        [DllImport("advapi32.dll", EntryPoint = "GetNamedSecurityInfoW", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern uint GetNamedSecurityInfoW(
                 string pObjectName,
                 SE_OBJECT_TYPE ObjectType,
                 SECURITY_INFORMATION SecurityInfo,
                 out IntPtr pSidOwner,
                 out IntPtr pSidGroup,
                 out IntPtr pDacl,
                 out IntPtr pSacl,
                 out IntPtr pSecurityDescriptor);

        [DllImport("Advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern void SetFileSecurity(string path, int type_of_sd, IntPtr sd);

        [DllImport("kernel32.dll", EntryPoint = "LocalFree", SetLastError = true)]
        public static extern IntPtr LocalFree(IntPtr handle);

        [DllImport("advapi32", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool ConvertSidToStringSid([MarshalAs(UnmanagedType.LPArray)] byte[] sid, out IntPtr ptrSid);

        // Call LocalFree to free SecurityDescriptor pointer.
        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool ConvertStringSecurityDescriptorToSecurityDescriptor(
               string StringSecurityDescriptor,
               uint StringSDRevision,
               out IntPtr SecurityDescriptor,
               out UIntPtr SecurityDescriptorSize);

        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool ConvertSecurityDescriptorToStringSecurityDescriptor(
            IntPtr SecurityDescriptor,
            uint StringSDRevision,
            SECURITY_INFORMATION SecurityInformation,
            out IntPtr StringSecurityDescriptor,
            out UIntPtr StringSecurityDescriptorLen);
        #endregion

        [DllImport("advapi32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool AdjustTokenPrivileges(IntPtr TokenHandle,
           [MarshalAs(UnmanagedType.Bool)]bool DisableAllPrivileges,
           ref TOKEN_PRIVILEGES NewState,
           UInt32 BufferLengthInBytes,
           ref TOKEN_PRIVILEGES PreviousState,
           out UInt32 ReturnLengthInBytes);

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool LookupPrivilegeValue(string lpSystemName, string lpName,
            ref LUID lpLuid);
    }
}
