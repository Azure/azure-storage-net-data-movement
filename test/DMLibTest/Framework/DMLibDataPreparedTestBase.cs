//------------------------------------------------------------------------------
// <copyright file="SearchPatternTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public class DMLibDataPreparedTestBase : DMLibTestBase
    {
        private static Dictionary<string, DMLibDataInfo> sourceDataInfos = new Dictionary<string, DMLibDataInfo>();
        private static Dictionary<string, DMLibDataInfo> destDataInfos = new Dictionary<string, DMLibDataInfo>();

        public static Dictionary<string, DMLibDataInfo> GetDataInfos(SourceOrDest sourceOrDest)
        {
            return SourceOrDest.Source == sourceOrDest ? sourceDataInfos : destDataInfos;
        }

        protected static DMLibDataInfo SourceDataInfo
        {
            get
            {
                return GetDataInfo(SourceOrDest.Source, DMLibTestContext.SourceType);
            }
        }

        protected static DMLibDataInfo DestDataInfo
        {
            get
            {
                return GetDataInfo(SourceOrDest.Dest, DMLibTestContext.DestType);
            }
        }

        protected static void PrepareSourceData(DMLibDataType dataType, DMLibDataInfo dataInfo)
        {
            PrepareData(SourceOrDest.Source, dataType, dataInfo);
        }

        protected static void PrepareDestData(DMLibDataType dataType, DMLibDataInfo dataInfo)
        {
            PrepareData(SourceOrDest.Dest, dataType, dataInfo);
        }

        protected static void SetSourceDataInfo(DMLibDataType dataType, DMLibDataInfo dataInfo)
        {
            sourceDataInfos[dataType.ToString()] = dataInfo;
        }

        protected static void SetDestDataInfo(DMLibDataType dataType, DMLibDataInfo dataInfo)
        {
            destDataInfos[dataType.ToString()] = dataInfo;
        }

        protected static void CleanupSourceData(DMLibDataType dataType)
        {
            CleanupData(SourceOrDest.Source, dataType);
        }

        protected static void CleanupDestData(DMLibDataType dataType)
        {
            CleanupData(SourceOrDest.Dest, dataType);
        }

        private static DMLibDataInfo GetDataInfo(SourceOrDest sourceOrDest, DMLibDataType dataType)
        {
            var dataInfos = GetDataInfos(sourceOrDest);

            if (!dataInfos.ContainsKey(dataType.ToString()))
            {
                throw new ArgumentException(
                    string.Format("{0} transfer data info of specified dataType doesn't exist: {1}", sourceOrDest == SourceOrDest.Source ? "Source" : "Destination", dataType.ToString()), "dataType");
            }

            return dataInfos[dataType.ToString()];
        }

        private static void PrepareData(SourceOrDest sourceOrDest, DMLibDataType dataType, DMLibDataInfo dataInfo)
        {
            var dataInfos = GetDataInfos(sourceOrDest);
            CleanupData(sourceOrDest, dataType);

            if (SourceOrDest.Source == sourceOrDest)
            {
                GetSourceAdaptor(dataType).GenerateData(dataInfo);
                SetSourceDataInfo(dataType, dataInfo);
            }
            else
            {
                GetDestAdaptor(dataType).GenerateData(dataInfo);
                SetDestDataInfo(dataType, dataInfo);
            }
        }

        private static void CleanupData(SourceOrDest sourceOrDest, DMLibDataType dataType)
        {
            if (SourceOrDest.Source == sourceOrDest)
            {
                GetSourceAdaptor(dataType).Cleanup();
            }
            else
            {
                GetDestAdaptor(dataType).Cleanup();
            }
        }

        #region Additional test attributes
        [ClassInitialize()]
        public static new void BaseClassInitialize(TestContext testContext)
        {
            DMLibTestBase.BaseClassInitialize(testContext);

            DMLibTestBase.CleanupSource = false;
        }

        [ClassCleanup()]
        public static new void BaseClassCleanup()
        {
            DMLibTestBase.BaseClassCleanup();
        }

        [TestInitialize()]
        public new void BaseTestInitialize()
        {
            base.BaseTestInitialize();
        }

        [TestCleanup()]
        public new void BaseTestCleanup()
        {
            base.BaseTestCleanup();
        }
        #endregion
    }
}
