//-----------------------------------------------------------------------------
// <copyright file="TransferManager.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//-----------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System.Globalization;
    using Microsoft.Azure.Storage.DataMovement.Interop;

    internal class FileToAzureFileNameResolver : INameResolver
    {
        private static char[] invalidPathChars = new char[] { '"', '\\', ':', '|', '<', '>', '*', '?' };

        protected static string EscapeInvalidCharacters(string fileName)
        {
            // Replace invalid characters with %HH, with HH being the hexadecimal
            // representation of the invalid character.
            foreach (char c in invalidPathChars)
            {
                fileName = fileName.Replace(c.ToString(), string.Format(CultureInfo.InvariantCulture, "%{0:X2}", (int)c));
            }

            return fileName;
        }

        public string ResolveName(TransferEntry sourceEntry)
        {
            if (CrossPlatformHelpers.IsWindows)
            {
                return sourceEntry.RelativePath.Replace('\\', '/');
            }
            else
            {
                return EscapeInvalidCharacters(sourceEntry.RelativePath);
            }
        }
    }
}
