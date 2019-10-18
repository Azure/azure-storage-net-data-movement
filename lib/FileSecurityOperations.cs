//------------------------------------------------------------------------------
// <copyright file="FileSecurityOperations.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Security.Principal;
    using System.Text;
    using Microsoft.Azure.Storage.DataMovement.Interop;

    internal static class FileSecurityOperations
    {
        private static long OwnerPrivilegeJobCount = 0;
        private static long SACLPrivilegeJobCount = 0;

        private static object OwnerPrivilegeLock = new object();
        private static object SACLPrivilegeLock = new object();

        private static bool OwnerPrivilegeEnabledOriginal = false;
        private static bool SACLPrivilegeEnabledOriginal = false;

        private static bool OwnerPrivilegeEnabled = false;
        private static bool SACLPrivilegeEnabled = false;

        public static void BeginTransferJob(PreserveSMBPermissions preserveSMBPermissions, bool setLocalFilePermission)
        {
            if (PreserveSMBPermissions.PreserveSACLPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveSACLPermission))
            {
                if (!SACLPrivilegeEnabledOriginal)
                {
                    lock (SACLPrivilegeLock)
                    {
                        if (SACLPrivilegeEnabledOriginal)
                        {
                            return;
                        }

                        ++SACLPrivilegeJobCount;

                        if (SACLPrivilegeEnabled)
                        {
                            return;
                        }

                        try
                        {
                            SACLPrivilegeEnabledOriginal = SetPrivilege(NativeMethods.SACLPrivilegeName, true);
                        }
                        catch (Exception)
                        {
                            --SACLPrivilegeJobCount;
                            throw;
                        }

                        SACLPrivilegeEnabled = true;

                        if (SACLPrivilegeEnabledOriginal)
                        {
                            --SACLPrivilegeJobCount;
                        }
                    }
                }
            }

            if (!setLocalFilePermission)
            {
                return;
            }

            if (PreserveSMBPermissions.PreserveOwnerPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveOwnerPermission))
            {
                if (!OwnerPrivilegeEnabledOriginal)
                {
                    lock (OwnerPrivilegeLock)
                    {
                        if (OwnerPrivilegeEnabledOriginal)
                        {
                            return;
                        }

                        ++OwnerPrivilegeJobCount;

                        if (OwnerPrivilegeEnabled)
                        {
                            return;
                        }

                        try
                        {
                            OwnerPrivilegeEnabledOriginal = SetPrivilege(NativeMethods.OwnerPrivilegeName, true);
                        }
                        catch (COMException)
                        { }
                        catch (TransferException)
                        { }

                        OwnerPrivilegeEnabled = true;

                        if (OwnerPrivilegeEnabledOriginal)
                        {
                            --OwnerPrivilegeJobCount;
                        }
                    }
                }
            }
        }

        public static void EndTransferJob(PreserveSMBPermissions preserveSMBPermissions, bool setLocalFilePermission)
        {
            if (PreserveSMBPermissions.PreserveSACLPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveSACLPermission))
            {
                if (!SACLPrivilegeEnabledOriginal)
                {
                    lock (SACLPrivilegeLock)
                    {
                        if (SACLPrivilegeEnabledOriginal)
                        {
                            return;
                        }

                        --SACLPrivilegeJobCount;

                        if (0 == SACLPrivilegeJobCount)
                        {
                            try
                            {
                                SetPrivilege(NativeMethods.SACLPrivilegeName, false);
                            }
                            catch (TransferException)
                            { }
                            catch (COMException)
                            { }
                        }

                        SACLPrivilegeEnabled = false;
                    }
                }
            }

            if (!setLocalFilePermission)
            {
                return;
            }

            if (PreserveSMBPermissions.PreserveOwnerPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveOwnerPermission))
            {
                if (!OwnerPrivilegeEnabledOriginal)
                {
                    lock (OwnerPrivilegeLock)
                    {
                        if (OwnerPrivilegeEnabledOriginal)
                        {
                            return;
                        }

                        --OwnerPrivilegeJobCount;

                        if (0 == OwnerPrivilegeJobCount)
                        {
                            try
                            {
                                SetPrivilege(NativeMethods.OwnerPrivilegeName, false);
                            }
                            catch (TransferException)
                            { }
                            catch (COMException)
                            { }
                        }

                        OwnerPrivilegeEnabled = false;
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Interoperability", "CA1404:CallGetLastErrorImmediatelyAfterPInvoke")]
        private static bool SetPrivilege(
               string securityPrivilege,
               bool bEnablePrivilege   // to enable or disable privilege
               )
        {
            var identity = WindowsIdentity.GetCurrent(TokenAccessLevels.AdjustPrivileges | TokenAccessLevels.Query);
            var accessToken = identity.Token;

            NativeMethods.TOKEN_PRIVILEGES tp;
            NativeMethods.LUID luid = new NativeMethods.LUID();

            if (!NativeMethods.LookupPrivilegeValue(
                    null,            // lookup privilege on local system
                    securityPrivilege,   // privilege to lookup 
                    ref luid))        // receives LUID of privilege
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            tp.PrivilegeCount = 1;
            tp.Privileges = new NativeMethods.LUID_AND_ATTRIBUTES[1];
            tp.Privileges[0].Luid = luid;
            if (bEnablePrivilege)
                tp.Privileges[0].Attributes = NativeMethods.SE_PRIVILEGE_ENABLED;
            else
                tp.Privileges[0].Attributes = NativeMethods.SE_PRIVILEGE_DISABLED;

            NativeMethods.TOKEN_PRIVILEGES previousPrivileges = new NativeMethods.TOKEN_PRIVILEGES();
            uint previousPrivilegesLength = 0;

            // Enable the privilege or disable all privileges.
            bool succeeded = NativeMethods.AdjustTokenPrivileges(
                   accessToken,
                   false,
                   ref tp,
                   1024,
                   ref previousPrivileges,
                   out previousPrivilegesLength);

            if (!succeeded)
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
            else if (Marshal.GetLastWin32Error() == NativeMethods.ERROR_NOT_ALL_ASSIGNED)
            {
                throw new TransferException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.FailedToEnablePrivilegeException,
                        securityPrivilege));
            }

            if ((previousPrivileges.Privileges.Count() == 1)
                && (previousPrivileges.Privileges[0].Luid.LowPart == luid.LowPart)
                && (previousPrivileges.Privileges[0].Luid.HighPart == luid.HighPart)
                && (previousPrivileges.Privileges[0].Attributes == NativeMethods.SE_PRIVILEGE_ENABLED))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static void SetFileSecurity(string filePath, string portableSDDL, PreserveSMBPermissions preserveSMBPermissions)
        {
            if (PreserveSMBPermissions.None == preserveSMBPermissions) return;

            if (string.IsNullOrEmpty(portableSDDL)) return;

            IntPtr pSecurityDescriptor = new IntPtr();
            UIntPtr securityDescriptorLength = new UIntPtr();

            if (!NativeMethods.ConvertStringSecurityDescriptorToSecurityDescriptor(portableSDDL,
                                                                         NativeMethods.SDDL_REVISION_1,
                                                                         out pSecurityDescriptor,
                                                                         out securityDescriptorLength))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            try
            {
                NativeMethods.SetFileSecurity(filePath,
                        (int)(ToSecurityInfo(preserveSMBPermissions)),
                    pSecurityDescriptor);

                int errorCode = Marshal.GetLastWin32Error();

                if (errorCode == NativeMethods.ERROR_PRIVILEGE_NOT_HELD)
                {
                    throw new TransferException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            Resources.PrivilegeRequiredException,
                            "https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow"));
                }
                else if (errorCode != 0)
                {
                    Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
            finally
            {
                NativeMethods.LocalFree(pSecurityDescriptor);
            }
        }

        public static string GetFilePortableSDDL(
            string filePath,
            PreserveSMBPermissions preserveSMBPermissions)
        {
            if (preserveSMBPermissions == PreserveSMBPermissions.None)
            {
                return null;
            }

            string sddl = GetFileSDDL(filePath, ToSecurityInfo(preserveSMBPermissions));
            string userName = GetUserName();
            string domainSid = GetDomainSid(userName);
            return GetPortableSDDL(sddl, domainSid);
        }

        private static NativeMethods.SECURITY_INFORMATION ToSecurityInfo(PreserveSMBPermissions preserveSMBPermissions)
        {
            uint securityInfo = 0;

            if (PreserveSMBPermissions.PreserveOwnerPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveOwnerPermission))
                securityInfo |= (uint)NativeMethods.SECURITY_INFORMATION.OWNER_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.PreserveGroupPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveGroupPermission))
                securityInfo |= (uint)NativeMethods.SECURITY_INFORMATION.GROUP_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.PreserveDACLPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveDACLPermission))
                securityInfo |= (uint)NativeMethods.SECURITY_INFORMATION.DACL_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.PreserveSACLPermission == (preserveSMBPermissions & PreserveSMBPermissions.PreserveSACLPermission))
                securityInfo |= (uint)NativeMethods.SECURITY_INFORMATION.SACL_SECURITY_INFORMATION;

            return (NativeMethods.SECURITY_INFORMATION)(securityInfo);
        }

        private static string GetUserName()
        {
            StringBuilder userNameBuffer = new StringBuilder(64);
            int nSize = 64;
            if (!NativeMethods.GetUserName(userNameBuffer, ref nSize))
            {
                if (Marshal.GetLastWin32Error() == NativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    userNameBuffer.EnsureCapacity(nSize);

                    if (!NativeMethods.GetUserName(userNameBuffer, ref nSize))
                    {
                        throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                }
                else
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }

            return userNameBuffer.ToString();
        }

        private static string GetPortableSDDL(string sddl, string domainSid)
        {
            StringBuilder portableSddl = new StringBuilder();

            // Map of well known domain relative idenifiers to their full SID
            Dictionary<string, string> domainRelativeIdentifiersMap = new Dictionary<string, string>();

            domainRelativeIdentifiersMap.Add("LA", domainSid + "-500");
            domainRelativeIdentifiersMap.Add("LG", domainSid + "-501");
            domainRelativeIdentifiersMap.Add("CA", domainSid + "-517");
            domainRelativeIdentifiersMap.Add("DA", domainSid + "-512");
            domainRelativeIdentifiersMap.Add("DD", domainSid + "-516");
            domainRelativeIdentifiersMap.Add("DU", domainSid + "-513");
            domainRelativeIdentifiersMap.Add("DG", domainSid + "-514");
            domainRelativeIdentifiersMap.Add("DC", domainSid + "-515");
            domainRelativeIdentifiersMap.Add("SA", domainSid + "-518");
            domainRelativeIdentifiersMap.Add("EA", domainSid + "-519");
            domainRelativeIdentifiersMap.Add("PA", domainSid + "-520");
            domainRelativeIdentifiersMap.Add("RS", domainSid + "-553");
            domainRelativeIdentifiersMap.Add("ED", domainSid + "-498");
            domainRelativeIdentifiersMap.Add("RO", domainSid + "-521");

            // Parse the sddl and convert into portable format i.e. replace each detected RID into full SID
            {
                int ownerIndex = sddl.IndexOf("O:", StringComparison.Ordinal);
                int groupIndex = sddl.IndexOf("G:", StringComparison.Ordinal);
                int daclIndex = sddl.IndexOf("D:", StringComparison.Ordinal);
                int saclIndex = sddl.IndexOf("S:", StringComparison.Ordinal);

                List<int> indexes = new List<int>();

                if (ownerIndex != -1)
                {
                    indexes.Add(ownerIndex);
                }

                if (groupIndex != -1)
                {
                    indexes.Add(groupIndex);
                }

                if (daclIndex != -1)
                {
                    indexes.Add(daclIndex);
                }

                if (saclIndex != -1)
                {
                    indexes.Add(saclIndex);
                }

                indexes.Add(sddl.Length);

                indexes.Sort();

                for (int index = 0; index < indexes.Count - 1; index++)
                {
                    int beginIndex = indexes[index];
                    int endIndex = indexes[index + 1];

                    if ((beginIndex == ownerIndex) ||
                        (beginIndex == groupIndex))
                    {
                        // Fix owner / group
                        // Owner and Group have the same format
                        bool sddlCopied = false;

                        foreach (var keypair in domainRelativeIdentifiersMap)
                        {
                            int ridIndex = sddl.IndexOf(keypair.Key, beginIndex, StringComparison.Ordinal);

                            if ((ridIndex != -1) && (ridIndex < endIndex))
                            {
                                sddlCopied = true;
                                portableSddl.Append(sddl, beginIndex, ridIndex - beginIndex);
                                portableSddl.Append(keypair.Value);

                                beginIndex = ridIndex + keypair.Key.Length;
                            }
                        }

                        if (!sddlCopied)
                        {
                            portableSddl.Append(sddl, beginIndex, endIndex - beginIndex);
                        }
                    }
                    else if ((beginIndex == daclIndex) ||
                        (beginIndex == saclIndex))
                    {
                        // Fix dacl / sacl
                        // DACL and SACL have the same format
                        // Each acl format is: ace_type;ace_flags;rights;object_guid;inherit_object_guid;account_sid;(resource_attribute)
                        int aclStartIndex = sddl.IndexOf('(', beginIndex);
                        bool sddlCopied = false;

                        if ((aclStartIndex != -1) && (aclStartIndex < endIndex))
                        {
                            portableSddl.Append(sddl, beginIndex, aclStartIndex - beginIndex);
                        }

                        while ((aclStartIndex != -1) && (aclStartIndex < endIndex))
                        {
                            sddlCopied = false;

                            int aclEndIndex = sddl.IndexOf(')', aclStartIndex + 1);

                            if (aclStartIndex == -1)
                            {
                                Console.Error.WriteLine("Invalid SDDL");
                                return "";
                            }

                            // Fix the account Sid if it have domain RID
                            // format is: ace_type;ace_flags;rights;object_guid;inherit_object_guid;account_sid;(resource_attribute) 
                            int accountSidStart = sddl.IndexOf(';', aclStartIndex + 1);
                            accountSidStart = sddl.IndexOf(';', accountSidStart + 1);
                            accountSidStart = sddl.IndexOf(';', accountSidStart + 1);
                            accountSidStart = sddl.IndexOf(';', accountSidStart + 1);
                            accountSidStart = sddl.IndexOf(';', accountSidStart + 1);

                            portableSddl.Append(sddl, aclStartIndex, accountSidStart - aclStartIndex);

                            foreach (var keypair in domainRelativeIdentifiersMap)
                            {
                                int ridIndex = sddl.IndexOf(keypair.Key, accountSidStart, StringComparison.Ordinal);

                                if ((ridIndex != -1) && (ridIndex < aclEndIndex))
                                {
                                    sddlCopied = true;
                                    portableSddl.Append(sddl, accountSidStart, ridIndex - accountSidStart);
                                    portableSddl.Append(keypair.Value);
                                    portableSddl.Append(sddl, ridIndex + keypair.Key.Length, aclEndIndex - (ridIndex + keypair.Key.Length) + 1);
                                }
                            }

                            if (!sddlCopied)
                            {
                                sddlCopied = true;
                                portableSddl.Append(sddl, accountSidStart, (aclEndIndex - accountSidStart) + 1);
                            }

                            aclStartIndex = sddl.IndexOf('(', aclEndIndex);
                        }

                        if (!sddlCopied)
                        {
                            portableSddl.Append(sddl, beginIndex, endIndex - beginIndex);
                        }
                    }
                }
            }

            return portableSddl.ToString();
        }

        private static string GetFileSDDL(
            string filePath,
            NativeMethods.SECURITY_INFORMATION securityInfo)
        {
            // Note: to get the SACL, special permissions are needed. Refer https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-getsecurityinfo
            IntPtr pZero = IntPtr.Zero;
            IntPtr pSid = pZero;
            IntPtr psd = pZero;

            uint errorReturn = NativeMethods.GetNamedSecurityInfoW(
                filePath,
                NativeMethods.SE_OBJECT_TYPE.SE_FILE_OBJECT,
                securityInfo,
                out pSid, out pZero, out pZero, out pZero, out psd);

            if (errorReturn == NativeMethods.ERROR_PRIVILEGE_NOT_HELD)
            {
                throw new TransferException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.PrivilegeRequiredException,
                        "https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-getsecurityinfo"));
            }
            else if (errorReturn != 0)
            {
                throw new Win32Exception((int)errorReturn);
            }

            try
            {
                IntPtr sdString = IntPtr.Zero;
                UIntPtr sd_string_size_ptr = new UIntPtr();
                bool success = NativeMethods.ConvertSecurityDescriptorToStringSecurityDescriptor(psd, NativeMethods.SDDL_REVISION_1, securityInfo, out sdString, out sd_string_size_ptr);
                try
                {
                    return Marshal.PtrToStringAuto(sdString);
                }
                finally
                {
                    Marshal.FreeHGlobal(sdString);
                }
            }
            finally
            {
                NativeMethods.LocalFree(psd);
            }
        }

        private static string GetDomainSid(string accountName)
        {
            uint sidLength = 0;
            uint domainNameLength = 0;
            byte[] sid = null;
            StringBuilder domainName = new StringBuilder();
            NativeMethods.SID_NAME_USE peUse;

            // Query buffer size requirement by passing in NULL for Sid and ReferencedDomainName
            if (!NativeMethods.LookupAccountName(null, // lpSystemName
                                   accountName,
                                   null,  // Sid
                                   ref sidLength,
                                   domainName,  // ReferencedDomainName
                                   ref domainNameLength,
                                   out peUse))
            {
                int errorCode = Marshal.GetLastWin32Error();

                if (errorCode != NativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }

                sid = new byte[sidLength];
                domainName.EnsureCapacity((int)domainNameLength);

                if (!NativeMethods.LookupAccountName(null,
                               accountName,
                               sid,
                               ref sidLength,
                               domainName,
                               ref domainNameLength,
                               out peUse))
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
            else
            {
                throw new TransferException("Unexpected LookupAccountName success");
            }

            // Get domain Sid from User Sid
            uint domainSidLength = 0;
            byte[] domainSid = null;
            if (!NativeMethods.GetWindowsAccountDomainSid(sid, domainSid, ref domainSidLength))
            {
                int errorCode = Marshal.GetLastWin32Error();

                if (errorCode != NativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }

                domainSid = new byte[domainSidLength];

                if (!NativeMethods.GetWindowsAccountDomainSid(sid, domainSid, ref domainSidLength))
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
            }
            else
            {
                throw new TransferException("Unexpected GetWindowsAccountDomainSid success");
            }

            // Get string corresponding to SID
            IntPtr domainSidPtr;
            if (!NativeMethods.ConvertSidToStringSid(domainSid, out domainSidPtr))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            try
            {
                return Marshal.PtrToStringAuto(domainSidPtr);
            }
            finally
            {
                NativeMethods.LocalFree(domainSidPtr);
            }
        }
    }
}
