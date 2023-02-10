using System;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using IOFile = System.IO.File;

namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal static class UploadHelper
    {
        internal const string KeyName = "TapiMD5Value";

        public static Task AddMd5ToMetadataDelegate(object source, object destination)
        {
            Task.Yield();

            ((CloudBlockBlob)destination).Metadata[KeyName] = CalculateMd5((string)source);

            return Task.CompletedTask;
        }

        private static string CalculateMd5(string filename)
        {
            using var md5 = MD5.Create();
            using var stream = IOFile.OpenRead(filename);

            return Convert.ToBase64String(md5.ComputeHash(stream));
        }
    }
}