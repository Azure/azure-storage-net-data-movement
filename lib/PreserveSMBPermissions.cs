//------------------------------------------------------------------------------
// <copyright file="PreserveSMBPermissions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;

    /// <summary>
    /// Enum to indicate what permission would be perserved DataMovement Library.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SMB")]
    [Flags]
    public enum PreserveSMBPermissions : int
    {
        /// <summary>
        /// Indicate to not preserve any permission
        /// </summary>
        None = 0,

        /// <summary>
        /// To preserve Owner permission.
        /// In some cases, it requires to enable SeRestorePrivilege to set owner info to local file. 
        /// See <c>https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow</c> for details.
        /// To set owner permission to local file during downloading, the process needs to run with an account who has been assigned
        /// the privilege, for example run the process with administrator account.
        /// </summary>
        Owner = 0x00000001,

        /// <summary>
        /// To preserve Group permission.
        /// </summary>
        Group = 0x00000002,

        /// <summary>
        /// To preserve DACL permission.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "DACL")]
        DACL = 0x00000004,

        /// <summary>
        /// To preserve SACL permission.
        /// It requires to enable SeSecurityPrivilege to get or set SACL from or to local file.
        /// See <c>https://docs.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow</c> for details.
        /// To get or set owner permission from or to local file, the process needs to run with an account who has been assigned
        /// the privilege, for example run the process with administrator account.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "SACL")]
        SACL = 0x00000008
    }
}
