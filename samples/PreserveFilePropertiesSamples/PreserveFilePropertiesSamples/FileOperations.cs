//------------------------------------------------------------------------------
// <copyright file="FileOperations.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace PreserveFilePropertiesSamples
{
    using System;
    using System.IO;

    static class FileOperations
    {
        public static void GetFileProperties(
            string path, 
            out DateTimeOffset? creationTime, 
            out DateTimeOffset? lastWriteTime, 
            out FileAttributes? fileAttributes,
            bool isDirectory = false)
        {
            fileAttributes = File.GetAttributes(path);

            if (isDirectory)
            {
                creationTime = Directory.GetCreationTimeUtc(path);
                lastWriteTime = Directory.GetLastWriteTimeUtc(path);
            }
            else
            {
                creationTime = File.GetCreationTimeUtc(path);
                lastWriteTime = File.GetLastWriteTimeUtc(path);
            }
        }

        public static void SetFileProperties(string path, 
            DateTimeOffset creationTime, 
            DateTimeOffset lastWriteTime,
            FileAttributes fileAttributes,
            bool isDirectory = false)
        {
            File.SetAttributes(path, fileAttributes);

            if (isDirectory)
            {
                Directory.SetCreationTimeUtc(path, creationTime.UtcDateTime);
                Directory.SetLastWriteTimeUtc(path, lastWriteTime.UtcDateTime);
            }
            else
            {
                File.SetCreationTimeUtc(path, creationTime.UtcDateTime);
                File.SetLastWriteTimeUtc(path, lastWriteTime.UtcDateTime);
            }
        }
    }
}
