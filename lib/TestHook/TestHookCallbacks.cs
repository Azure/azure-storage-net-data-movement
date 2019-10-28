using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Storage.DataMovement
{
#if DEBUG
    public static class TestHookCallbacks
    {
        public static Action<string, FileAttributes> SetFileAttributesCallback;
        public static Func<string, FileAttributes> GetFileAttributesCallback;

        public static Action<string, string, PreserveSMBPermissions> SetFilePermissionsCallback;
        public static Func<string, PreserveSMBPermissions, string> GetFilePermissionsCallback;

        public static bool UnderTesting
        {
            get;
            set;
        }
    }
#endif
}
