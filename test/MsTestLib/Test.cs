//------------------------------------------------------------------------------
// <copyright file="Test.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{
    public static class Test
    {
        public static string TestDataFile;
        public static TestConfig Data;
        public static TestLogger Logger;
        public static int TestCount = 0;
        public static int FailCount = 0;
        public static int SkipCount = 0;

        public static string FullClassName = string.Empty;
        public static string MethodName = string.Empty;

        public static int ErrorCount = 0;
        public static int SkipErrorCount = 0;

        public static List<String> FailedCases = null;
        public static List<String> SkippedCases = null;

        public static void Init()
        {
            Init(TestDataFile);
        }

        public static void Init(string testDataFile)
        {
            Data = new TestConfig(testDataFile);
            Logger = new TestLogger(Data);
            FailedCases = new List<string>();
            SkippedCases = new List<string>();
        }

        public static void Close()
        {
            Logger.Close();
        }

        public static void Info(
            string msg,
            params object[] objToLog)
        {
            Logger.Info(msg, objToLog);
        }

        public static void Warn(
        string msg,
        params object[] objToLog)
        {
            Logger.Warning(msg, objToLog);
        }

        public static void Verbose(
        string msg,
        params object[] objToLog)
        {
            Logger.Verbose(msg, objToLog);
        }

        public static void Error(
        string msg,
        params object[] objToLog)
        {
            ErrorCount++;
            Logger.Error(msg, objToLog);
        }

        public static void SkipError(
        string msg,
        params object[] objToLog)
        {
            SkipErrorCount++;
            Logger.Error(msg, objToLog);
        }

        public static void Assert(bool condition,
            string msg,
            params object[] objToLog)
        {
            if (condition)
            {
                Verbose("[Assert Pass] " + msg, objToLog);
            }
            else
            {
                Error("[Assert Fail] " + msg, objToLog);
            }
        }

        public static void Start(string testClass, string testMethod)
        {
            TestCount++;
            ErrorCount = 0;
            SkipErrorCount = 0;
            Logger.StartTest(testClass + "." + testMethod);
            Test.FullClassName = testClass;
            Test.MethodName = testMethod;
        }

        public static void End(string testClass, string testMethod)
        {
            if (ErrorCount == 0 && SkipErrorCount == 0)
            {
                Logger.EndTest(testClass + "." + testMethod, TestResult.PASS);
            }
            else if (SkipErrorCount > 0)
            {
                SkipCount++;
                Logger.EndTest(testClass + "." + testMethod, TestResult.SKIP);
                AssertFail(string.Format("The case is skipped since Test init fail. Please check the detailed case log."));
                SkippedCases.Add(String.Format("{0}.{1}", testClass, testMethod));
            }
            else
            {
                FailCount++;
                Logger.EndTest(testClass + "." + testMethod, TestResult.FAIL);
                AssertFail(string.Format("There " + (ErrorCount > 1 ? "are {0} errors" : "is {0} error") + " so the case fails. Please check the detailed case log.", ErrorCount));
                FailedCases.Add(String.Format("{0}.{1}", testClass, testMethod));
            }

        }

        public static AssertFailDelegate AssertFail;

    }

    public delegate void AssertFailDelegate(string msg);
}
