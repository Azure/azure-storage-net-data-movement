//------------------------------------------------------------------------------
// <copyright file="DMLibTestConstants.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using Microsoft.Azure.Storage.RetryPolicies;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using System.IO;

    public static class Tag
    {
        public const string BVT = "bvt";
        public const string Function = "function";
        public const string Stress = "stress";
        public const string Performance = "perf";
    }

    public static class Protocol
    {
        public const string Http = "http";

        public static string Https
        {
            get
            {
                if (DMLibTestHelper.DisableHttps())
                {
                    return "http";
                }

                return "https";
            }
        }
    }

    public static class BlobType
    {
        public const string Page = "page";
        public const string Block = "block";
        public const string Append = "append";
    }

    public static class DMLibTestConstants
    {
        public const string ConnStr = "StorageConnectionString";
        public const string ConnStr2 = "StorageConnectionString2";
        public const string DestEncryptionScope = "DestinationEncryptionScope";
        public static readonly int DefaultNC = TransferManager.Configurations.ParallelOperations;
        public static readonly int DefaultBlockSize = 4 * 1024 * 1024; //4MB
        public static readonly int LimitedSpeedNC = 4;
        public static readonly TimeSpan DefaultExecutionTimeOut = TimeSpan.FromMinutes(15);

        private static Random random = new Random();

        public static bool SupportUNCPath = true;
        
        public static IRetryPolicy DefaultRetryPolicy
        {
            get
            {
                return new LinearRetry(TimeSpan.FromSeconds(90), 3);
            }
        }

        public static int FlatFileCount
        {
            get
            {
                int flatFileCount;
                try
                {
                    flatFileCount = int.Parse(Test.Data.Get("FlatFileCount"));
                }
                catch
                {
                    flatFileCount = 20;
                }

                Test.Verbose("Flat file count: {0}", flatFileCount);
                return flatFileCount;
            }
        }

        public static int RecursiveFolderWidth
        {
            get
            {
                int recursiveFolderWidth;
                try
                {
                    recursiveFolderWidth = int.Parse(Test.Data.Get("RecursiveFolderWidth"));
                }
                catch
                {
                    recursiveFolderWidth = random.Next(3, 5);
                }

                Test.Verbose("Recursive folder width: {0}", recursiveFolderWidth);
                return recursiveFolderWidth;
            }
        }

        public static int RecursiveFolderDepth
        {
            get
            {
                int recursiveFolderDepth;
                try
                {
                    recursiveFolderDepth = int.Parse(Test.Data.Get("RecursiveFolderDepth"));
                }
                catch
                {
                    recursiveFolderDepth = random.Next(3, 5);
                }

                Test.Verbose("Recursive folder depth: {0}", recursiveFolderDepth);
                return recursiveFolderDepth;
            }
        }

        static DMLibTestConstants()
        {
            SupportUNCPath = false;
            if (CrossPlatformHelpers.IsWindows)
            {
                try
                {
                    LongPath.GetFullPath(LongPath.ToUncPath("F:\\"));
                    SupportUNCPath = true;
                }
                catch (Exception)
                { }
            }
        }
    }
}
