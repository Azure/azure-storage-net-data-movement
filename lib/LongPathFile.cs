//------------------------------------------------------------------------------
// <copyright file="LongPathFile.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;
    using Microsoft.WindowsAzure.Storage.DataMovement.Interop;

    internal static partial class LongPathFile
    {
        public static bool Exists(string path)
        {
#if DOTNET5_4
            string longFilePath = path;
            if (Interop.CrossPlatformHelpers.IsWindows)
            {
                longFilePath = LongPath.ToUncPath(longFilePath);
            }
            return File.Exists(longFilePath);
#else
            try
            {
                if (String.IsNullOrEmpty(path))
                    return false;
                path = LongPath.ToUncPath(path);
                bool success = NativeMethods.PathFileExistsW(path);
                if (!success)
                {
                    NativeMethods.ThrowExceptionForLastWin32ErrorIfExists(new int[] { 0, NativeMethods.ERROR_DIRECTORY_NOT_FOUND, NativeMethods.ERROR_FILE_NOT_FOUND });
                }
                var fileAttributes = Microsoft.WindowsAzure.Storage.DataMovement.LongPathFile.GetAttributes(path);
                return success && (FileAttributes.Directory != (fileAttributes & FileAttributes.Directory));
            }
            catch (ArgumentException) { }
            catch (NotSupportedException) { }  // Security can throw this on ":"
            catch (System.Security.SecurityException) { }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }

            return false;
#endif
        }
    }
}
