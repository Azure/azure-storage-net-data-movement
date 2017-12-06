//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    public enum StopDMLibType
    {
        None,
        Kill,
        TestHookCtrlC,
        BreakNetwork
    }

    public enum SourceOrDest
    {
        Source,
        Dest,
    }

    public abstract class MultiDirectionTestBase<TDataInfo, TDataType>
        where TDataInfo : IDataInfo
        where TDataType : struct
    {
        public const string SourceRoot = "sourceroot";
        public const string DestRoot = "destroot";
        public const string SourceFolder = "sourcefolder";
        public const string DestFolder = "destfolder";

        protected static Random random = new Random();

        private static Dictionary<string, DataAdaptor<TDataInfo>> sourceAdaptors = new Dictionary<string, DataAdaptor<TDataInfo>>();
        private static Dictionary<string, DataAdaptor<TDataInfo>> destAdaptors = new Dictionary<string, DataAdaptor<TDataInfo>>();

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        public static string NetworkShare
        {
            get;
            set;
        }

        public static bool CleanupSource
        {
            get;
            set;
        }

        public static bool CleanupDestination
        {
            get;
            set;
        }

        public static DataAdaptor<TDataInfo> SourceAdaptor
        {
            get
            {
                return GetSourceAdaptor(MultiDirectionTestContext<TDataType>.SourceType);
            }
        }

        public static DataAdaptor<TDataInfo> DestAdaptor
        {
            get
            {
                return GetDestAdaptor(MultiDirectionTestContext<TDataType>.DestType);
            }
        }

        public static void BaseClassInitialize(TestContext testContext)
        {
            Test.Info("ClassInitialize");
#if !DNXCORE50
            Test.FullClassName = testContext.FullyQualifiedTestClassName;
#endif

            MultiDirectionTestBase<TDataInfo, TDataType>.CleanupSource = true;
            MultiDirectionTestBase<TDataInfo, TDataType>.CleanupDestination = true;

            NetworkShare = Test.Data.Get("NetworkFolder");
        }

        public static void BaseClassCleanup()
        {
            Test.Info("ClassCleanup");
            //DeleteAllLocations(sourceAdaptors);
            //DeleteAllLocations(destAdaptors);
            Test.Info("ClassCleanup done.");
        }

        private static void DeleteAllLocations(Dictionary<string, DataAdaptor<TDataInfo>> adaptorDic)
        {
            Parallel.ForEach(adaptorDic, pair =>
            {
                try
                {
                    pair.Value.DeleteLocation();
                }
                catch
                {
                    Test.Warn("Fail to delete location for data adaptor: {0}", pair.Key);
                }
            });
        }

        public virtual void BaseTestInitialize()
        {
#if !DNXCORE50
            Test.Start(TestContext.FullyQualifiedTestClassName, TestContext.TestName);
#endif
            Test.Info("TestInitialize");

            MultiDirectionTestInfo.Cleanup();
        }

        public virtual void BaseTestCleanup()
        {
            if (Test.ErrorCount > 0)
            {
                MultiDirectionTestInfo.Print();
            }

            Test.Info("TestCleanup");
#if !DNXCORE50
            Test.End(TestContext.FullyQualifiedTestClassName, TestContext.TestName);
#endif

            try
            {
                this.CleanupData();
                MultiDirectionTestBase<TDataInfo, TDataType>.SourceAdaptor.Reset();
                MultiDirectionTestBase<TDataInfo, TDataType>.DestAdaptor.Reset();
            }
            catch
            {
                // ignore exception
            }
        }

        public virtual void CleanupData()
        {
            this.CleanupData(
                MultiDirectionTestBase<TDataInfo, TDataType>.CleanupSource,
                MultiDirectionTestBase<TDataInfo, TDataType>.CleanupDestination);
        }

        protected void CleanupData(bool cleanupSource, bool cleanupDestination)
        {
            if (cleanupSource)
            {
                MultiDirectionTestBase<TDataInfo, TDataType>.SourceAdaptor.Cleanup();
            }

            if (cleanupDestination)
            {
                MultiDirectionTestBase<TDataInfo, TDataType>.DestAdaptor.Cleanup();
            }
        }

        protected static string GetLocationKey(TDataType dataType)
        {
            return dataType.ToString();
        }

        public static DataAdaptor<TDataInfo> GetSourceAdaptor(TDataType dataType)
        {
            string key = MultiDirectionTestBase<TDataInfo, TDataType>.GetLocationKey(dataType);

            if (!sourceAdaptors.ContainsKey(key))
            {
                throw new KeyNotFoundException(
                    string.Format("Can't find key of source data adaptor. DataType:{0}.", dataType.ToString()));
            }

            return sourceAdaptors[key];
        }

        public static DataAdaptor<TDataInfo> GetDestAdaptor(TDataType dataType)
        {
            string key = MultiDirectionTestBase<TDataInfo, TDataType>.GetLocationKey(dataType);

            if (!destAdaptors.ContainsKey(key))
            {
                throw new KeyNotFoundException(
                    string.Format("Can't find key of destination data adaptor. DataType:{0}.", dataType.ToString()));
            }

            return destAdaptors[key];
        }

        protected static void SetSourceAdaptor(TDataType dataType, DataAdaptor<TDataInfo> adaptor)
        {
            string key = MultiDirectionTestBase<TDataInfo, TDataType>.GetLocationKey(dataType);
            sourceAdaptors[key] = adaptor;
        }

        protected static void SetDestAdaptor(TDataType dataType, DataAdaptor<TDataInfo> adaptor)
        {
            string key = MultiDirectionTestBase<TDataInfo, TDataType>.GetLocationKey(dataType);
            destAdaptors[key] = adaptor;
        }

        public abstract bool IsCloudService(TDataType dataType);

        public static CredentialType GetRandomCredentialType()
        {
            int credentialCount = Enum.GetNames(typeof(CredentialType)).Length;
            int randomNum = MultiDirectionTestBase<TDataInfo, TDataType>.random.Next(0, credentialCount);

            CredentialType result;
            switch (randomNum)
            {
                case 0:
                    result = CredentialType.None;
                    break;
                case 1:
                    result = CredentialType.Public;
                    break;
                case 2:
                    result = CredentialType.Key;
                    break;
                case 3:
                    result = CredentialType.SAS;
                    break;
                default:
                    result = CredentialType.EmbeddedSAS;
                    break;
            }

            Test.Info("Random credential type: {0}", result.ToString());
            return result;
        }

        protected static string GetRelativePath(string basePath, string fullPath)
        {
            string normalizedBasePath = MultiDirectionTestBase<TDataInfo, TDataType>.NormalizePath(basePath);
            string normalizedFullPath = MultiDirectionTestBase<TDataInfo, TDataType>.NormalizePath(fullPath);

            int index = normalizedFullPath.IndexOf(normalizedBasePath);

            if (index < 0)
            {
                return null;
            }

            return normalizedFullPath.Substring(index + normalizedBasePath.Length);
        }

        protected static string NormalizePath(string path)
        {
            if (path.StartsWith("\"") && path.EndsWith("\""))
            {
                path = path.Substring(1, path.Length - 2);
            }

            try
            {
                var uri = new Uri(path);
                return uri.GetComponents(UriComponents.Path, UriFormat.Unescaped);
            }
            catch (UriFormatException)
            {
                return path;
            }
        }
    }

    public enum CredentialType
    {
        None = 0,
        Public,
        Key,
        SAS,
        EmbeddedSAS,
    }
}
