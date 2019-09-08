//------------------------------------------------------------------------------
// <copyright file="MD5Wrapper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Security.Cryptography;

    internal class MD5Wrapper : IDisposable
    {
#if DOTNET5_4
        private IncrementalHash hash = null;
        private NativeMD5 nativeMd5 = null;
        private bool useV1MD5 = true;
#else         
        private MD5 hash = null;
#endif

        [SuppressMessage("Microsoft.Cryptographic.Standard", "CA5350:MD5CannotBeUsed", Justification = "Used as a hash, not encryption")]
        internal MD5Wrapper()
        {
#if DOTNET5_4
            this.useV1MD5 = CloudStorageAccount.UseV1MD5 || !Interop.CrossPlatformHelpers.IsWindows;
            if (useV1MD5)
            {
                this.hash = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            }
            else
            {
                this.nativeMd5 = new NativeMD5();
            }
#else

            if (CloudStorageAccount.UseV1MD5)
            {
                this.hash = new MD5CryptoServiceProvider();
            }
            else
            {
                this.hash = new NativeMD5();
            }
#endif
        }

        /// <summary>
        /// Calculates an on-going hash using the input byte array.
        /// </summary>
        /// <param name="input">The input array used for calculating the hash.</param>
        /// <param name="offset">The offset in the input buffer to calculate from.</param>
        /// <param name="count">The number of bytes to use from input.</param>
        internal void UpdateHash(byte[] input, int offset, int count)
        {
            if (count > 0)
            {
#if DOTNET5_4
                if (useV1MD5)
                {
                    this.hash.AppendData(input, offset, count);
                }
                else
                {
                    this.nativeMd5.TransformBlock(input, offset, count, null, 0);
                }
#else
                this.hash.TransformBlock(input, offset, count, null, 0);
#endif
            }
        }

        /// <summary>
        /// Retrieves the string representation of the hash. (Completes the creation of the hash).
        /// </summary>
        /// <returns>String representation of the computed hash value.</returns>
        internal string ComputeHash()
        {
#if DOTNET5_4
            if (useV1MD5)
            {
                return Convert.ToBase64String(this.hash.GetHashAndReset());
            }
            else
            {
                this.nativeMd5.TransformFinalBlock(new byte[0], 0, 0);
                return Convert.ToBase64String(this.nativeMd5.Hash);
            }
#else
            this.hash.TransformFinalBlock(new byte[0], 0, 0);
            return Convert.ToBase64String(this.hash.Hash);
#endif
        }

        public void Dispose()
        {
            if (this.hash != null)
            {
                this.hash.Dispose();
                this.hash = null;
            }

#if DOTNET5_4
            if (this.nativeMd5 != null)
            {
                this.nativeMd5.Dispose();
                this.nativeMd5 = null;
            }
#endif
        }
    }
}
