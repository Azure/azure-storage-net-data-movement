//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    internal static partial class NativeMethods
    {
        private const string MemInfoPath = @"/proc/meminfo";

        /// <summary>
        /// Retrieves system memory information, as stored in /proc/meminfo
        /// </summary>
        /// <returns>Memory information from /proc/meminfo</returns>
        internal static Dictionary<string, ulong> GetLinuxMemoryInfo()
        {
            var ret = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);

            // Check for /proc/meminfo's existence
            if (File.Exists(MemInfoPath))
            {
                try
                {
                    // Read the file and parse it into a dictionary
                    var memInfoLines = File.ReadAllLines(MemInfoPath);
                    foreach (var line in memInfoLines)
                    {
                        var splitLine = line.Split(new[] { ' ', '\t', ':' }, StringSplitOptions.RemoveEmptyEntries);
                        if (splitLine.Length < 2) continue;

                        // Memory statistics are all always reported as kB. If that should change in the future,
                        // it will be necessary to consider the third splitLine element to understand the units of memory
                        // being reported. That value would either have to be factored into values or provided as an additional
                        // data member, possibly by making the dictionary values Tuple<ulong, string> instead of ulongs.
                        //
                        // Currently, the third element of splitLine is ignored since memory units are hard-coded (as kB).

                        ulong lineValue;
                        if (!ret.ContainsKey(splitLine[0]) && ulong.TryParse(splitLine[1], out lineValue))
                        {
                            ret.Add(splitLine[0], lineValue);
                        }
                    }
                }
                catch (UnauthorizedAccessException)
                {
                    // /proc/meminfo could be inaccessible for some reason
                }
            }

            return ret;
        }
    }
}