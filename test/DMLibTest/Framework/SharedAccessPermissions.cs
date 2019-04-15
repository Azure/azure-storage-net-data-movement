//------------------------------------------------------------------------------
// <copyright file="SharedAccessPermissions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;

    [Flags]
    public enum SharedAccessPermissions
    {
        None = 0,
        Read = 1,
        Write = 2,
        Delete = 4,
        List = 8,
        Query = 16,
        Add = 32,
        Update = 64,
    }

    public static class SharedAccessPermissionsExtensions
    {
        public const SharedAccessPermissions LeastPermissionDest = SharedAccessPermissions.Write | SharedAccessPermissions.Read;
        public const SharedAccessPermissions LeastPermissionSource = SharedAccessPermissions.List | SharedAccessPermissions.Read;
        public const SharedAccessPermissions LeastPermissionSourceList = SharedAccessPermissions.List;

        public static SharedAccessBlobPermissions ToBlobPermissions(this SharedAccessPermissions sap)
        {
            return (SharedAccessBlobPermissions)Enum.Parse(typeof(SharedAccessBlobPermissions), sap.ToString());
        }

        public static SharedAccessFilePermissions ToFilePermissions(this SharedAccessPermissions sap)
        {
            return (SharedAccessFilePermissions)Enum.Parse(typeof(SharedAccessFilePermissions), sap.ToString());
        }

        public static SharedAccessPermissions ToCommonPermissions(this Enum specificPermissions)
        {
            return (SharedAccessPermissions)Enum.Parse(typeof(SharedAccessPermissions), specificPermissions.ToString());
        }
    }

}
