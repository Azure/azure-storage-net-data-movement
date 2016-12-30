//------------------------------------------------------------------------------
// <copyright file="AzureNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Name resolver class for translating Azure blob/file names.
    /// </summary>
    internal abstract class AzureNameResolver : INameResolver
    {
        /// <summary>
        /// Default delimiter used as a directory separator in blob name.
        /// </summary>
        public const char DefaultDelimiter = '/';

        /// <summary>
        /// Here lists special characters in regular expression,
        /// those characters need to be escaped in regular expression.
        /// </summary>
        private static HashSet<char> regexSpecialCharacters = new HashSet<char>(
            new char[] { '*', '^', '$', '(', ')', '+', '|', '[', ']', '"', '.', '/', '?', '\\' });

        /// <summary>
        /// Regular expression string format for replacing delimiters 
        /// that we consider as directory separators:
        /// <para>Translate delimiters to '\' if it is:
        /// not the first or the last character in the file name 
        /// and not following another delimiter</para>
        /// <example>/folder1//folder2/ with '/' as delimiter gets translated to: /folder1\/folder2/ </example>.
        /// </summary>
        private static string translateDelimitersRegexFormat = "(?<=[^{0}]+){0}(?=.+)";

        /// <summary>
        /// Regular expression for replacing delimiters that we consider as directory separators:
        /// <para>Translate delimiters to '\' if it is:
        /// not the first or the last character in the file name 
        /// and not following another delimiter</para>
        /// <example>/folder1//folder2/ with '/' as delimiter gets translated to: /folder1\/folder2/ </example>.
        /// </summary>
        private Regex translateDelimitersRegex;

        private char delimiter;

        public AzureNameResolver(char? delimiter)
        {
            this.delimiter = null == delimiter ? DefaultDelimiter : delimiter.Value;

            // In azure storage, it will transfer every '\' to '/', so '\' won't be a delimiter.
            if (regexSpecialCharacters.Contains(this.delimiter))
            {
                string delimiterTemp = "\\" + this.delimiter;
                this.translateDelimitersRegex = new Regex(string.Format(CultureInfo.InvariantCulture, translateDelimitersRegexFormat, delimiterTemp), RegexOptions.Compiled);
            }
            else
            {
                this.translateDelimitersRegex = new Regex(string.Format(CultureInfo.InvariantCulture, translateDelimitersRegexFormat, this.delimiter), RegexOptions.Compiled);
            }
        }

        protected abstract string DirSeparator
        {
            get;
        }

        protected abstract char[] InvalidPathChars
        {
            get;
        }

        public string ResolveName(TransferEntry sourceEntry)
        {
            // 1) Unescape original string, original string is UrlEncoded.
            // 2) Replace Azure directory separator with Windows File System directory separator.
            // 3) Trim spaces at the end of the file name.
            string destinationRelativePath = EscapeInvalidCharacters(this.TranslateDelimiters(sourceEntry.RelativePath).TrimEnd(new char[] { ' ' }));

            // Split into path + filename parts.
            int lastSlash = destinationRelativePath.LastIndexOf(this.DirSeparator, StringComparison.Ordinal);

            string destinationFileName;
            string destinationPath;

            if (-1 == lastSlash)
            {
                destinationPath = string.Empty;
                destinationFileName = destinationRelativePath;
            }
            else
            {
                destinationPath = destinationRelativePath.Substring(0, lastSlash + 1);
                destinationFileName = destinationRelativePath.Substring(lastSlash + 1);
            }

            // Append snapshot time to filename.
            AzureBlobEntry blobEntry = sourceEntry as AzureBlobEntry;
            if (blobEntry != null)
            {
                destinationFileName = Utils.AppendSnapShotTimeToFileName(destinationFileName, blobEntry.Blob.SnapshotTime);
            }

            // Combine path and filename back together again.
            destinationRelativePath = this.CombinePath(destinationPath, destinationFileName);

            return destinationRelativePath;
        }

        protected abstract string CombinePath(string folder, string name);

        protected virtual string TranslateDelimiters(string source)
        {
            // Transform delimiters used for directory separators to windows file system directory separator "\"
            // or azure file separator "/" according to destination location type.
            return this.translateDelimitersRegex.Replace(source, this.DirSeparator);
        }

        protected virtual string EscapeInvalidCharacters(string fileName)
        {
            if (null != this.InvalidPathChars)
            {
                // Replace invalid characters with %HH, with HH being the hexadecimal
                // representation of the invalid character.
                foreach (char c in this.InvalidPathChars)
                {
                    fileName = fileName.Replace(c.ToString(), string.Format(CultureInfo.InvariantCulture, "%{0:X2}", (int)c));
                }
            }

            return fileName;
        }
    }
}
