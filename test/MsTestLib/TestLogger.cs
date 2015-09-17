//------------------------------------------------------------------------------
// <copyright file="TestLogger.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace MS.Test.Common.MsTestLib
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// the wrapper for the loggers
    /// </summary>
    public class TestLogger
    {
        public List<ILogger> Loggers;
        private Object loggersLock = new Object();

        public TestLogger()
        {
            Loggers = new List<ILogger>();
        }

        public TestLogger(TestConfig testConfig)
        {
            Loggers = new List<ILogger>();
            Init(testConfig);
        }

        public bool LogVerbose = false;
        public bool LogInfo = true;
        public bool LogWarning = false;
        public bool LogError = true;

        public void Init(TestConfig testConfig)
        {
            bool consoleLogger = false;
            bool fileLogger = true;
            bool.TryParse(testConfig.TestParams["consolelogger"], out consoleLogger);
            bool.TryParse(testConfig.TestParams["filelogger"], out fileLogger);

            string logfileName = testConfig.TestParams["logfilename"];

            if (consoleLogger)
            {
                Loggers.Add(new ConsoleLogger());
            }

            if (fileLogger)
            {
                string fileNameString = logfileName + ".txt";
                Loggers.Add(new FileLogger(fileNameString));
            }

            bool.TryParse(testConfig.TestParams["loginfo"], out LogInfo);
            bool.TryParse(testConfig.TestParams["logverbose"], out LogVerbose);
            bool.TryParse(testConfig.TestParams["logerror"], out LogError);
            bool.TryParse(testConfig.TestParams["logwarning"], out LogWarning);
        }

        public void Error(
            string msg,
            params object[] objToLog)
        {
            this.ForEachLogger((logger) =>
            {
                if (LogError)
                {
                    logger.WriteError(msg, objToLog);
                }
            });
        }

        public void Info(
            string msg,
            params object[] objToLog)
        {
            this.ForEachLogger((logger) =>
            {
                if (LogInfo)
                {
                    logger.WriteInfo(msg, objToLog);
                }
            });
        }

        public void Warning(
            string msg,
            params object[] objToLog)
        {
            this.ForEachLogger((logger) =>
            {
                if (LogWarning)
                {
                    logger.WriteWarning(msg, objToLog);
                }
            });
        }

        public void Verbose(
            string msg,
            params object[] objToLog)
        {
            this.ForEachLogger((logger) =>
            {
                if (LogVerbose)
                {
                    logger.WriteVerbose(msg, objToLog);
                }
            });
        }

        public void StartTest(string testId)
        {
            this.ForEachLogger((logger) =>
            {
                logger.StartTest(testId);
            });
        }

        public void EndTest(string testId, TestResult testResult)
        {
            this.ForEachLogger((logger) =>
            {
                logger.EndTest(testId, testResult);
            });
        }

        public void Close()
        {
            this.ForEachLogger((logger) =>
            {
                logger.Close();
            });
        }

        private void ForEachLogger(Action<ILogger> action)
        {
            lock (this.loggersLock)
            {
                foreach (ILogger logger in Loggers)
                {
                    action(logger);
                }
            }
        }
    }
}
