//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestInfo.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System.Collections.Generic;
    using MS.Test.Common.MsTestLib;

    public class MultiDirectionTestInfo
    {
        public static List<IDataInfo> GeneratedSourceDataInfos = new List<IDataInfo>();
        public static List<IDataInfo> GeneratedDestDataInfos = new List<IDataInfo>();

        public static void Cleanup()
        {
            MultiDirectionTestInfo.GeneratedSourceDataInfos.Clear();
            MultiDirectionTestInfo.GeneratedDestDataInfos.Clear();
        }

        public static void Print()
        {
            Test.Info("-----Source Data-----");
            foreach (var sourceDataInfo in MultiDirectionTestInfo.GeneratedSourceDataInfos)
            {
                MultiDirectionTestHelper.PrintTransferDataInfo(sourceDataInfo);
            }

            Test.Info("-----Dest Data-----");
            foreach (var destDataInfo in MultiDirectionTestInfo.GeneratedDestDataInfos)
            {
                MultiDirectionTestHelper.PrintTransferDataInfo(destDataInfo);
            }
        }
    }
}
