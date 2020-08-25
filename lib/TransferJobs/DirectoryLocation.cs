//------------------------------------------------------------------------------
// <copyright file="DirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;

#if BINARY_SERIALIZATION
    [Serializable]
#else
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal class DirectoryLocation : TransferLocation
#if BINARY_SERIALIZATION
        , ISerializable
#endif // BINARY_SERIALIZATION
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

#if BINARY_SERIALIZATION
        private DirectoryLocation(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new System.ArgumentNullException("info");
            }

            this.DirectoryPath = info.GetString(DirPathName);
        }
#endif // BINARY_SERIALIZATION

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
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.DirectoryPath;
            }
        }

        /// <summary>
        /// Gets path to the local directory location.
        /// </summary>
#if !BINARY_SERIALIZATION
        [DataMember]
#endif
        public string DirectoryPath
        {
            get;
            private set;
        }

#if BINARY_SERIALIZATION
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
#endif // BINARY_SERIALIZATION

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
#if DOTNET5_4
            DirectoryInfo di = new DirectoryInfo(this.DirectoryPath);
            di.Create();
#else
            string longDirectoryPath = this.DirectoryPath.ToLongPath();

            if (!LongPathDirectory.Exists(longDirectoryPath))
            {
                LongPathDirectory.CreateDirectory(longDirectoryPath);
            }
#endif
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
