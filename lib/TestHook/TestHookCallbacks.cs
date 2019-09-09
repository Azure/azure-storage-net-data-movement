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
    }
#endif
}
