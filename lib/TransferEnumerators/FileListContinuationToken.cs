//------------------------------------------------------------------------------
// <copyright file="FileListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    sealed class FileListContinuationToken : ListContinuationToken, ISerializable
    {
        private const string FilePathName = "FilePath";

        public FileListContinuationToken(string filePath)
        {
            this.FilePath = filePath;
        }

        private FileListContinuationToken(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.FilePath = info.GetString(FilePathName);
        }

        /// <summary>
        /// Gets relative path of the last listed file.
        /// </summary>
        public string FilePath
        {
            get;
            private set;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            info.AddValue(FilePathName, this.FilePath, typeof(string));
        }
    }
}
