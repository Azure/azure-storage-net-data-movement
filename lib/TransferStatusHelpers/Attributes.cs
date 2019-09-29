//------------------------------------------------------------------------------
// <copyright file="Attributes.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Storage.File;

    internal class Attributes
    {
        /// <summary>
        /// Gets or sets the cache-control value stored for blob/azure file.
        /// </summary>
        public string CacheControl { get; set; }

        /// <summary>
        /// Gets or sets the content-disposition value stored for blob/azure file.
        /// </summary>
        public string ContentDisposition { get; set; }

        /// <summary>
        /// Gets or sets the content-encoding value stored for blob/azure file.
        /// </summary>
        public string ContentEncoding { get; set; }
        
        /// <summary>
        /// Gets or sets the content-language value stored for blob/azure file.
        /// </summary>
        public string ContentLanguage { get; set; }
        
        /// <summary>
        /// Gets or sets the content-MD5 value stored for blob/azure file.
        /// </summary>
        public string ContentMD5 { get; set; }
        
        /// <summary>
        /// Gets or sets the content-type value stored for blob/azure file.
        /// </summary>
        public string ContentType { get; set; }

        /// <summary>
        /// Gets or sets the user-defined metadata for blob/azure file.
        /// </summary>
        public IDictionary<string, string> Metadata { get; set; }

        /// <summary>
        /// Gets or sets the file attributes for azure file.
        /// </summary>
        public CloudFileNtfsAttributes? CloudFileNtfsAttributes { get; set; }

        /// <summary>
        /// Gets or sets the file's creation time for azure file.
        /// </summary>
        public DateTimeOffset? CreationTime { get; set; }
        
        /// <summary>
        /// Gets or sets the file's last write time for azure file.
        /// </summary>
        public DateTimeOffset? LastWriteTime { get; set; }

        /// <summary>
        /// Gets or sets a value to indicate whether to overwrite all attribute on destination,
        /// or keep its original value if it's not set.
        /// </summary>
        public bool OverWriteAll { get; set; }
    }
}
