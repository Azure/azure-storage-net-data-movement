//------------------------------------------------------------------------------
// <copyright file="FileSecurityOperations.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace PreserveFilePropertiesSamples
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Security.Principal;
    using System.Text;
    using Microsoft.Azure.Storage.DataMovement;

    class FileSecurityOperations
    {
        private static bool OwnerPrivilegeEnabledOriginal = false;
        private static bool SACLPrivilegeEnabledOriginal = false;

        /// <summary>
        /// Enable privileges required for getting/setting premissions from/to local file. 
        /// </summary>
        /// <param name="preserveSMBPermissions">Permissions types to be preserved.</param>
        /// <param name="setLocalFilePermission">Indicating whether to set permission to local file. 
        /// Setting owner info to local file would need specific privilege, while getting owner info doesn't require this privilege.</param>
        public static void EnableRequiredPrivileges(PreserveSMBPermissions preserveSMBPermissions, bool setLocalFilePermission)
        {
            if (PreserveSMBPermissions.SACL == (preserveSMBPermissions & PreserveSMBPermissions.SACL))
            {
                if (!SACLPrivilegeEnabledOriginal)
                {
                    try
                    {
                        SACLPrivilegeEnabledOriginal = SetPrivilege(FileSecurityNativeMethods.SACLPrivilegeName, true);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
            }

            if (!setLocalFilePermission)
            {
                return;
            }

            if (PreserveSMBPermissions.Owner == (preserveSMBPermissions & PreserveSMBPermissions.Owner))
            {
                if (!OwnerPrivilegeEnabledOriginal)
                {

                    try
                    {
                        OwnerPrivilegeEnabledOriginal = SetPrivilege(FileSecurityNativeMethods.OwnerPrivilegeName, true);
                    }
                    catch (COMException)
                    {
                        // Ignore exception here
                        // Under some condition, setting owner info to local file will still success even when there's no SeRestorePrivilege privilege.
                        // So here only try to enable the privilege. Later, if it fails to set owner info to local file, reports error for the specific file.
                        return;
                    }
                    catch (TransferException)
                    {
                        // Ignore exception here
                        // Under some condition, setting owner info to local file will still success even when there's no SeRestorePrivilege privilege.
                        // So here only try to enable the privilege. Later, if it fails to set owner info to local file, reports error for the specific file.
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Restore privileges enabled before for getting/setting local file permissions.
        /// </summary>
        /// <param name="preserveSMBPermissions">Permissions types to be preserved.</param>
        /// <param name="setLocalFilePermission">Indicating whether to set permission to local file. 
        /// Setting owner info to local file would need specific privilege, while getting owner info doesn't require this privilege.</param>
        public static void RestorePrivileges(PreserveSMBPermissions preserveSMBPermissions, bool setLocalFilePermission)
        {
            if (PreserveSMBPermissions.SACL == (preserveSMBPermissions & PreserveSMBPermissions.SACL))
            {
                if (!SACLPrivilegeEnabledOriginal)
                {
                    try
                    {
                        SetPrivilege(FileSecurityNativeMethods.SACLPrivilegeName, false);
                    }
                    catch (Win32Exception)
                    {
                        // Ignore the exception
                        // Here just try to clear up the privileges which may have been enabled at the very beginning of a tranfer job.
                        // Failure on disabling the privilege won't impact transfer result.
                    }
                    catch (COMException)
                    {
                        // Ignore the exception
                        // Here just try to clear up the privileges which may have been enabled at the very beginning of a tranfer job.
                        // Failure on disabling the privilege won't impact transfer result.
                    }
                }
            }

            if (!setLocalFilePermission)
            {
                return;
            }

            if (PreserveSMBPermissions.Owner == (preserveSMBPermissions & PreserveSMBPermissions.Owner))
            {
                if (!OwnerPrivilegeEnabledOriginal)
                {
                    try
                    {
                        SetPrivilege(FileSecurityNativeMethods.OwnerPrivilegeName, false);
                    }
                    catch (Win32Exception)
                    {
                        // Ignore the exception
                        // Here just try to clear up the privileges which may have been enabled at the very beginning of a tranfer job.
                        // Failure on disabling the privilege won't impact transfer result.
                    }
                    catch (COMException)
                    {
                        // Ignore the exception
                        // Here just try to clear up the privileges which may have been enabled at the very beginning of a tranfer job.
                        // Failure on disabling the privilege won't impact transfer result.
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

            FileSecurityNativeMethods.TOKEN_PRIVILEGES tp;
            FileSecurityNativeMethods.LUID luid = new FileSecurityNativeMethods.LUID();

            if (!FileSecurityNativeMethods.LookupPrivilegeValue(
                    null,            // lookup privilege on local system
                    securityPrivilege,   // privilege to lookup 
                    ref luid))        // receives LUID of privilege
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            tp.PrivilegeCount = 1;
            tp.Privileges = new FileSecurityNativeMethods.LUID_AND_ATTRIBUTES[1];
            tp.Privileges[0].Luid = luid;
            if (bEnablePrivilege)
                tp.Privileges[0].Attributes = FileSecurityNativeMethods.SE_PRIVILEGE_ENABLED;
            else
                tp.Privileges[0].Attributes = FileSecurityNativeMethods.SE_PRIVILEGE_DISABLED;

            FileSecurityNativeMethods.TOKEN_PRIVILEGES previousPrivileges = new FileSecurityNativeMethods.TOKEN_PRIVILEGES();
            uint previousPrivilegesLength = 0;

            // Enable the privilege or disable all privileges.
            bool succeeded = FileSecurityNativeMethods.AdjustTokenPrivileges(
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
            else if (Marshal.GetLastWin32Error() == FileSecurityNativeMethods.ERROR_NOT_ALL_ASSIGNED)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }

            if ((previousPrivileges.Privileges.Count() == 1)
                && (previousPrivileges.Privileges[0].Luid.LowPart == luid.LowPart)
                && (previousPrivileges.Privileges[0].Luid.HighPart == luid.HighPart)
                && (previousPrivileges.Privileges[0].Attributes == FileSecurityNativeMethods.SE_PRIVILEGE_ENABLED))
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

            if (!FileSecurityNativeMethods.ConvertStringSecurityDescriptorToSecurityDescriptor(portableSDDL,
                                                                         FileSecurityNativeMethods.SDDL_REVISION_1,
                                                                         out pSecurityDescriptor,
                                                                         out securityDescriptorLength))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            try
            {
                FileSecurityNativeMethods.SetFileSecurity(filePath,
                        (int)(ToSecurityInfo(preserveSMBPermissions)),
                    pSecurityDescriptor);

                int errorCode = Marshal.GetLastWin32Error();
                int hresult = Marshal.GetHRForLastWin32Error();

                if ((errorCode == FileSecurityNativeMethods.ERROR_PRIVILEGE_NOT_HELD)
                    || (errorCode == FileSecurityNativeMethods.ERROR_ACCESS_DENIED)
                    || (errorCode == FileSecurityNativeMethods.ERROR_INVALID_OWNER))
                {
                    throw new Win32Exception(errorCode);
                }

                if (errorCode != 0)
                {
                    Marshal.ThrowExceptionForHR(hresult);
                }
            }
            finally
            {
                FileSecurityNativeMethods.LocalFree(pSecurityDescriptor);
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

        private static FileSecurityNativeMethods.SECURITY_INFORMATION ToSecurityInfo(PreserveSMBPermissions preserveSMBPermissions)
        {
            uint securityInfo = 0;

            if (PreserveSMBPermissions.Owner == (preserveSMBPermissions & PreserveSMBPermissions.Owner))
                securityInfo |= (uint)FileSecurityNativeMethods.SECURITY_INFORMATION.OWNER_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.Group == (preserveSMBPermissions & PreserveSMBPermissions.Group))
                securityInfo |= (uint)FileSecurityNativeMethods.SECURITY_INFORMATION.GROUP_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.DACL == (preserveSMBPermissions & PreserveSMBPermissions.DACL))
                securityInfo |= (uint)FileSecurityNativeMethods.SECURITY_INFORMATION.DACL_SECURITY_INFORMATION;

            if (PreserveSMBPermissions.SACL == (preserveSMBPermissions & PreserveSMBPermissions.SACL))
                securityInfo |= (uint)FileSecurityNativeMethods.SECURITY_INFORMATION.SACL_SECURITY_INFORMATION;

            return (FileSecurityNativeMethods.SECURITY_INFORMATION)(securityInfo);
        }

        private static string GetUserName()
        {
            StringBuilder userNameBuffer = new StringBuilder(64);
            int nSize = 64;
            if (!FileSecurityNativeMethods.GetUserName(userNameBuffer, ref nSize))
            {
                if (Marshal.GetLastWin32Error() == FileSecurityNativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    userNameBuffer.EnsureCapacity(nSize);

                    if (!FileSecurityNativeMethods.GetUserName(userNameBuffer, ref nSize))
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
            Dictionary<string, string> domainRelativeIdentifiersMap = new Dictionary<string, string>
            {
                { "LA", domainSid + "-500" },
                { "LG", domainSid + "-501" },
                { "CA", domainSid + "-517" },
                { "DA", domainSid + "-512" },
                { "DD", domainSid + "-516" },
                { "DU", domainSid + "-513" },
                { "DG", domainSid + "-514" },
                { "DC", domainSid + "-515" },
                { "SA", domainSid + "-518" },
                { "EA", domainSid + "-519" },
                { "PA", domainSid + "-520" },
                { "RS", domainSid + "-553" },
                { "ED", domainSid + "-498" },
                { "RO", domainSid + "-521" }
            };

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
            FileSecurityNativeMethods.SECURITY_INFORMATION securityInfo)
        {
            // Note: to get the SACL, special permissions are needed. Refer https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-getsecurityinfo
            IntPtr pZero = IntPtr.Zero;
            IntPtr pSid = pZero;
            IntPtr psd = pZero;

            uint errorReturn = FileSecurityNativeMethods.GetNamedSecurityInfoW(
                filePath,
                FileSecurityNativeMethods.SE_OBJECT_TYPE.SE_FILE_OBJECT,
                securityInfo,
                out pSid, out pZero, out pZero, out pZero, out psd);

            if (errorReturn != 0)
            {
                throw new Win32Exception((int)errorReturn);
            }

            try
            {
                IntPtr sdString = IntPtr.Zero;
                UIntPtr sd_string_size_ptr = new UIntPtr();
                bool success = FileSecurityNativeMethods.ConvertSecurityDescriptorToStringSecurityDescriptor(psd, FileSecurityNativeMethods.SDDL_REVISION_1, securityInfo, out sdString, out sd_string_size_ptr);
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
                FileSecurityNativeMethods.LocalFree(psd);
            }
        }

        private static string GetDomainSid(string accountName)
        {
            uint sidLength = 0;
            uint domainNameLength = 0;
            byte[] sid = null;
            StringBuilder domainName = new StringBuilder();
            FileSecurityNativeMethods.SID_NAME_USE peUse;

            // Query buffer size requirement by passing in NULL for Sid and ReferencedDomainName
            if (!FileSecurityNativeMethods.LookupAccountName(null, // lpSystemName
                                   accountName,
                                   null,  // Sid
                                   ref sidLength,
                                   domainName,  // ReferencedDomainName
                                   ref domainNameLength,
                                   out peUse))
            {
                int errorCode = Marshal.GetLastWin32Error();

                if (errorCode != FileSecurityNativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }

                sid = new byte[sidLength];
                domainName.EnsureCapacity((int)domainNameLength);

                if (!FileSecurityNativeMethods.LookupAccountName(null,
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
            if (!FileSecurityNativeMethods.GetWindowsAccountDomainSid(sid, domainSid, ref domainSidLength))
            {
                int errorCode = Marshal.GetLastWin32Error();

                if (errorCode != FileSecurityNativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                }

                domainSid = new byte[domainSidLength];

                if (!FileSecurityNativeMethods.GetWindowsAccountDomainSid(sid, domainSid, ref domainSidLength))
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
            if (!FileSecurityNativeMethods.ConvertSidToStringSid(domainSid, out domainSidPtr))
            {
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }

            try
            {
                return Marshal.PtrToStringAuto(domainSidPtr);
            }
            finally
            {
                FileSecurityNativeMethods.LocalFree(domainSidPtr);
            }
        }
    }
}
