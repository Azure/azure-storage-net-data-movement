//------------------------------------------------------------------------------
// <copyright file="AssemblyInitCleanup.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    [TestClass]
    public class AssemblyInitCleanup
    {
        [AssemblyInitialize]
        public static void TestInit(TestContext testContext)
        {
            // init loggers and load test config data
            String config = testContext.Properties["config"] as string;
            Test.Init(config);
            // set the assertfail delegate to report failure in VS
            Test.AssertFail = new AssertFailDelegate(Assert.Fail);
        }

        [AssemblyCleanup]
        public static void TestCleanup()
        {
            Test.Close();
        }
    }
}
