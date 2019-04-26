//------------------------------------------------------------------------------
// <copyright file="AzureBlobToAzureFileNameResolver.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Globalization;
    using System.Text;

    internal class AzureBlobToAzureFileNameResolver : AzureNameResolver
    {
        private bool defaultDelimiter;
        
        private static char[] invalidPathChars = new char[] {'"', '\\', ':', '|', '<', '>', '*', '?'};

        public AzureBlobToAzureFileNameResolver(char? delimiter)
            : base(delimiter)
        {
            defaultDelimiter = null == delimiter || delimiter.Value == AzureNameResolver.DefaultDelimiter;
        }

        protected override string DirSeparator
        {
            get
            {
                return "/";
            }
        }

        protected override char[] InvalidPathChars
        {
            get
            {
                return AzureBlobToAzureFileNameResolver.invalidPathChars;
            }
        }

        protected override string CombinePath(string folder, string name)
        {
            if (!string.IsNullOrEmpty(folder))
            {
                if (folder.EndsWith(this.DirSeparator, StringComparison.Ordinal))
                {
                    return string.Format(CultureInfo.CurrentCulture, "{0}{1}", folder, name);
                }
                else
                {
                    return string.Format(CultureInfo.CurrentCulture, "{0}/{1}", folder, name);
                }
            }

            return name;
        }

        protected override string TranslateDelimiters(string source)
        {
            if (this.defaultDelimiter)
            {
                return source;
            }

            return base.TranslateDelimiters(source);
        }

        protected override string EscapeInvalidCharacters(string fileName)
        {
            StringBuilder sb = new StringBuilder();
            char separator = this.DirSeparator.ToCharArray()[0];
            string escapedSeparator = string.Format(CultureInfo.InvariantCulture, "%{0:X2}", (int)separator);

            bool followSeparator = false;
            char[] fileNameChars = fileName.ToCharArray();
            int lastIndex = fileNameChars.Length - 1;

            for (int i = 0; i < fileNameChars.Length; ++i)
            {
                if (fileNameChars[i] == separator)
                {
                    if (followSeparator || (0 == i) || (lastIndex == i))
                    {
                        sb.Append(escapedSeparator);
                    }
                    else
                    {
                        sb.Append(fileNameChars[i]);
                    }

                    followSeparator = true;
                }
                else
                {
                    followSeparator = false;
                    sb.Append(fileNameChars[i]);
                }
            }

            fileName = sb.ToString();

            return base.EscapeInvalidCharacters(fileName);
        }
    }
}
