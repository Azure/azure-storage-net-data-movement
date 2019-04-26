//------------------------------------------------------------------------------
// <copyright file="NameResolverHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Globalization;
    using System.IO;

    /// <summary>
    /// Helper class for resolving file names
    /// </summary>
    internal static class NameResolverHelper
    {
        /// <summary>
        /// Append snapshot time to a file name.
        /// </summary>
        /// <param name="fileName">Original file name.</param>
        /// <param name="snapshotTime">Snapshot time to append.</param>
        /// <returns>A file name with appended snapshot time.</returns>
        public static string AppendSnapShotTimeToFileName(string fileName, DateTimeOffset? snapshotTime)
        {
            string resultName = fileName;

            if (snapshotTime.HasValue)
            {
                string pathAndFileNameNoExt = Path.ChangeExtension(fileName, null);
                string extension = Path.GetExtension(fileName);
                string timeStamp = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0:yyyy-MM-dd HHmmss fff}",
                    snapshotTime.Value);

                resultName = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} ({1}){2}",
                    pathAndFileNameNoExt,
                    timeStamp,
                    extension);
            }

            return resultName;
        }
    }
}
