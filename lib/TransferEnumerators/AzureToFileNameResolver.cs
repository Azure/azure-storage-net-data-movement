//------------------------------------------------------------------------------
// <copyright file="AzureToFileNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System.Collections.Generic;
    using System.IO;
    using Interop;
    using System.Globalization;
    using System.Text.RegularExpressions;

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

#if DOTNET5_4
        /// <summary>
        /// This is used to escape the first "/" or "/" following a "/".
        /// Like to escape "/abc///ac" to "%2Fabc/%2F%2Fac"
        /// </summary>
        private static Regex escapeDirSeparators = new Regex("^/|(?<=/)/|/$");
#endif

        public AzureToFileNameResolver(char? delimiter)
            : base(delimiter)
        { 
        }

        protected override string DirSeparator
        {
            get
            {
                if (CrossPlatformHelpers.IsWindows)
                {
                    return "\\";
                }
                else
                {
                    return "/";
                }
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

#if DOTNET5_4
        protected override string TranslateDelimiters(string source)
        {
            if (!CrossPlatformHelpers.IsWindows
                && this.Delimiter != '/')
            {
                source = source.Replace("/", "%2F");
            }

            return base.TranslateDelimiters(source);
        }

        protected override string EscapeInvalidCharacters(string fileName)
        {
            fileName = base.EscapeInvalidCharacters(fileName);

            if (!CrossPlatformHelpers.IsWindows)
            {
                return escapeDirSeparators.Replace(fileName, "%2F");
            }
            else
            {
                return fileName;
            }
        }
#endif

        private static char[] GetInvalidPathChars()
        {
            if (CrossPlatformHelpers.IsWindows)
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
            }
            else
            {
                invalidPathChars = new char[] { '\0' };
            }

            return invalidPathChars;
        }
    }
}
