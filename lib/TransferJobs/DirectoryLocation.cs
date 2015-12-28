//------------------------------------------------------------------------------
// <copyright file="DirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;

    [Serializable]
    internal class DirectoryLocation : TransferLocation, ISerializable
    {
        private const string DirPathName = "DirPath";

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryLocation"/> class.
        /// </summary>
        /// <param name="dirPath">Path to the local directory as a source/destination to be read from/written to in a transfer.</param>
        public DirectoryLocation(string dirPath)
        {
            if (null == dirPath)
            {
                throw new ArgumentNullException("dirPath");
            }

            if (string.IsNullOrWhiteSpace(dirPath))
            {
                throw new ArgumentException("Directory path should not be an empty string", "dirPath");
            }

            // Normalize directory path to end with back slash.
            if (!dirPath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                dirPath += Path.DirectorySeparatorChar;
            }

            this.DirectoryPath = dirPath;
        }

        private DirectoryLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.DirectoryPath = info.GetString(DirPathName);
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.LocalDirectory;
            }
        }

        /// <summary>
        /// Gets path to the local directory location.
        /// </summary>
        public string DirectoryPath
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

            info.AddValue(DirPathName, this.DirectoryPath, typeof(string));
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            DirectoryInfo di = new DirectoryInfo(this.DirectoryPath);
            di.Create();
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.DirectoryPath;
        }
    }
}
