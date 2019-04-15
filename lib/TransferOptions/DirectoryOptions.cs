//------------------------------------------------------------------------------
// <copyright file="DirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for directory transfer operation
    /// </summary>
    public class DirectoryOptions
    {
        /// <summary>
        /// Gets or sets a string that will be used to match against the names of files.
        /// </summary>
        /// <remarks>
        /// Behavior of SearchPattern match varies for different source directory types and setting of Recursive:
        /// When source is local directory path, SearchPattern is matched against source file name as standard wildcards. If 
        /// recuresive is set to false, only files directly under the source directory will be matched. Otherwise, all files in the
        /// sub-directory will be matched as well.
        /// 
        /// When source is Azure blob directory, if recuresive is set to true, SearchPattern is matched against source blob as name prefix.
        /// Otherwise, only Azure blob with the exact name specified by SearchPattern will be matched.
        /// 
        /// When source is Azure file directory, if recursive is set to true, SearchPattern is not supported. Otherwise, only Azure file 
        /// with the exact name specified by SearchPattern will be matched.
        /// 
        /// If SearchPattern is not specified, "*.*" will be used for local directory source while empty string for Azure blob/file
        /// directory. So please either specify the Search Pattern or set Recursive to true when source is Azure blob/file directory,
        /// otherwise, no blob/file will be matched.
        /// </remarks>
        public string SearchPattern { get; set; }

        /// <summary>
        /// Gets or sets a boolean that indicates whether to include subdirectories when doing a directory transfer operation.
        /// </summary>
        public bool Recursive { get; set; }
    }
}
