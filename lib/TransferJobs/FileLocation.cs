//------------------------------------------------------------------------------
// <copyright file="FileLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;

    [Serializable]
    internal class FileLocation : TransferLocation, ISerializable
    {
        private const string FilePathName = "FilePath";
        /// <summary>
        /// Initializes a new instance of the <see cref="FileLocation"/> class.
        /// </summary>
        /// <param name="filePath">Path to the local file as a source/destination to be read from/written to in a transfer.</param>
        public FileLocation(string filePath)
        {
            if (null == filePath)
            {
                throw new ArgumentNullException("filePath");
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("message, should not be an empty string", "filePath");
            }

            this.FilePath = filePath;
        }

        private FileLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.FilePath = info.GetString(FilePathName);
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.FilePath;
            }
        }

        /// <summary>
        /// Gets path to the local file location.
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

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            DirectoryInfo di = new DirectoryInfo(Path.GetDirectoryName(this.FilePath));
            DateTime time = di.LastAccessTime;
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.FilePath;
        }
    }
}
