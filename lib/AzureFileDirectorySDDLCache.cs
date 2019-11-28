//------------------------------------------------------------------------------
// <copyright file="AzureFileDirectorySDDLCache.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    // Cache for SDDL permission string and permission key mapping.
    // For uploading from local file directory to Azure File Directory, key is SDDL string, value is permission key.
    // For downloading from Azure File Directory to local file directory, key is permission, value is SDDL string.
    internal class AzureFileDirectorySDDLCache : IDisposable
    {
        private Dictionary<string, string> sddlPermissionKey = new Dictionary<string, string>();
        private Queue<string> dictionaryKeys = new Queue<string>();
        private ReaderWriterLockSlim cacheLock = new ReaderWriterLockSlim();
        private int itemCount = 0;
        private const int MaximumItemCount = 128;

        public AzureFileDirectorySDDLCache()
        {
        }

        /// <summary>
        /// To to get corresponding value from the cache by a key.
        /// </summary>
        /// <param name="key">Key of item in the cache. 
        /// For uploading scenario, it's portable SDDL gotten from local file.
        /// For downloading scenario, it's permission-key on Azure File Share.
        /// </param>
        /// <param name="value">Key of item in the cache. 
        /// For uploading scenario, it's permission-key on Azure File Share.
        /// For downloading scenario, it's portable SDDL gotten from local file.</param>
        /// <returns>Flag to indicate whether can get the item from cache.
        /// True: Key exists in cache, got corresponding value.
        /// False: Key does not exist in cache, set value to null. </returns>
        public bool TryGetValue(string key, out string value)
        {
            this.cacheLock.EnterReadLock();
            try
            {
                value = null;
                return sddlPermissionKey.TryGetValue(key, out value);
            }
            finally
            {
                this.cacheLock.ExitReadLock();
            }
        }

        /// <summary>
        /// To to add an item to the cache.
        /// </summary>
        /// <param name="key">Key of item in the cache. 
        /// For uploading scenario, it's portable SDDL gotten from local file.
        /// For downloading scenario, it's permission-key on Azure File Share.
        /// </param>
        /// <param name="value">Key of item in the cache. 
        /// For uploading scenario, it's permission-key on Azure File Share.
        /// For downloading scenario, it's portable SDDL gotten from local file.</param>
        /// <returns>Flag to indicate whether the item has been added into the cache.
        /// True: There's no the same key in cache, the item has been added into cache.
        /// False: There's already an item with the same key in cache, the item is not added into cache. </returns>
        public bool TryAddValue(string key, string value)
        {
            this.cacheLock.EnterWriteLock();
            try
            {
                string originValue = null;
                if (sddlPermissionKey.TryGetValue(key, out originValue))
                {
                    return false;
                }

                if ((this.itemCount + 1) > MaximumItemCount)
                {
                    string keyToBeRemoved = this.dictionaryKeys.Dequeue();
                    this.sddlPermissionKey.Remove(keyToBeRemoved);
                }
                else
                {
                    ++this.itemCount;
                }
                this.sddlPermissionKey.Add(key, value);
                this.dictionaryKeys.Enqueue(key);
                return true;
            }
            finally
            {
                this.cacheLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Public dispose method to release all resources owned.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Private dispose method to release managed/unmanaged objects.
        /// If disposing = true clean up managed resources as well as unmanaged resources.
        /// If disposing = false only clean up unmanaged resources.
        /// </summary>
        /// <param name="disposing">Indicates whether or not to dispose managed resources.</param>
        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.cacheLock)
                {
                    this.cacheLock.Dispose();
                    this.cacheLock = null;
                }
            }
        }
    }
}
