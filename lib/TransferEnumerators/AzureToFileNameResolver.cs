//------------------------------------------------------------------------------
// <copyright file="AzureToFileNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// Name resolver class for translating Azure file/blob names to Windows file names.
    /// </summary>
    internal class AzureToFileNameResolver : AzureNameResolver
    {
        /// <summary>
        /// Chars invalid for file name.
        /// </summary>
        private static char[] invalidFileNameChars = Path.GetInvalidFileNameChars();

        /// <summary>
        /// Chars invalid for path name.
        /// </summary>
        private static char[] invalidPathChars = AzureToFileNameResolver.GetInvalidPathChars();

        public AzureToFileNameResolver(char? delimiter)
            : base(delimiter)
        { 
        }

        protected override string DirSeparator
        {
            get
            {
                return "\\";
            }
        }

        protected override char[] InvalidPathChars
        {
            get
            {
                return AzureToFileNameResolver.invalidPathChars;
            }
        }

        protected override string CombinePath(string folder, string name)
        {
            return Path.Combine(folder, name);
        }

        private static char[] GetInvalidPathChars()
        {
            // Union InvalidFileNameChars and InvalidPathChars together
            // while excluding slash.
            HashSet<char> charSet = new HashSet<char>(Path.GetInvalidPathChars());

            foreach (char c in invalidFileNameChars)
            {
                if ('\\' == c || charSet.Contains(c))
                {
                    continue;
                }

                charSet.Add(c);
            }

            invalidPathChars = new char[charSet.Count];
            charSet.CopyTo(invalidPathChars);

            return invalidPathChars;
        }
    }
}
