//------------------------------------------------------------------------------
// <copyright file="FileLogger.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{
    public class FileLogger : ILogger
    {

        private System.IO.StreamWriter m_file;

#if DOTNET5_4
        public FileLogger()
        {
            // TODO is USER reliable on Linux/OS X?
            string fileName = Environment.GetEnvironmentVariable("USER") + "_" + Environment.GetEnvironmentVariable("COMPUTERNAME") + " " + DateTime.Now.ToString().Replace('/', '-').Replace(':', '_') + ".txt";
            var fileStream = new System.IO.FileStream(fileName.ToString(), System.IO.FileMode.OpenOrCreate);
            m_file = new System.IO.StreamWriter(fileStream, Encoding.UTF8);
        }
#else
        /// 
        /// <summary>
        /// Creates a new instance of this class
        /// </summary>
        /// <exception cref="System.IO.IOException" />
        /// 
        public FileLogger()
        {
            string fileName = Environment.UserName + "_" + Environment.MachineName + " " + DateTime.Now.ToString().Replace('/', '-').Replace(':', '_') + ".txt";
            m_file = new System.IO.StreamWriter(fileName.ToString(), true);
        }
#endif

        /// 
        /// <summary>
        /// Creates a new instance of this class
        /// </summary>
        /// <exception cref="System.IO.IOException" />
        /// <param name="fileName">File to which logs should be appended</param>
        /// 
        public FileLogger(string fileName)
            : this(fileName, true)
        {

        }

        /// 
        /// <summary>
        /// Creates a new instance of this class
        /// </summary>
        /// <exception cref="System.IO.IOException" />
        /// <param name="fileName">File to which logs should be written/appended</param>
        /// <param name="append">denotes whether the file is to be appended or over-written</param>
        /// 
        public FileLogger(string fileName, bool append)
        {
#if DOTNET5_4
            var fileStream = new System.IO.FileStream(fileName.ToString(), System.IO.FileMode.OpenOrCreate);
            m_file = new System.IO.StreamWriter(fileStream, Encoding.UTF8);
#else
            // Open the file and assign to member variable
            m_file = new System.IO.StreamWriter(fileName, append);
#endif
        }

        /// 
        /// <summary>
        /// Writes an error log
        /// </summary>
        /// <param name="msg">Format message string</param>
        /// <param name="exp">exception object</param>
        /// <param name="objToLog">Objects that need to be serialized in the message</param>
        /// 
        public void WriteError(
            string msg,
            params object[] objToLog)
        {
            DateTime dt = DateTime.Now;
            StringBuilder sBuilder = new StringBuilder("[ERROR][" + dt.ToLongTimeString() + "." + dt.Millisecond + "]");
            sBuilder.Append(MessageBuilder.FormatString(msg, objToLog));
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
        }

        /// 
        /// <summary>
        /// Writes a warn log
        /// </summary>
        /// <param name="msg">Format message string</param>
        /// <param name="objToLog">Objects that need to be serialized in the message</param>
        /// 
        public void WriteWarning(
            string msg,
            params object[] objToLog)
        {
            DateTime dt = DateTime.Now;
            StringBuilder sBuilder = new StringBuilder("[WARN][" + dt.ToLongTimeString() + "." + dt.Millisecond + "]");
            sBuilder.Append(MessageBuilder.FormatString(msg, objToLog));
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
        }

        /// 
        /// <summary>
        /// Writes an info log
        /// </summary>
        /// <param name="msg">Format message string</param>
        /// <param name="objToLog">Objects that need to be serialized in the message</param>
        /// 
        public void WriteInfo(
            string msg,
            params object[] objToLog)
        {
            DateTime dt = DateTime.Now;
            StringBuilder sBuilder = new StringBuilder("[INFO][" + dt.ToLongTimeString() + "." + dt.Millisecond + "]");
            sBuilder.Append(MessageBuilder.FormatString(msg, objToLog));
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
        }

        /// 
        /// <summary>
        /// Writes a verbose log
        /// </summary>
        /// <param name="msg">Format message string</param>
        /// <param name="objToLog">Objects that need to be serialized in the message</param>
        /// 
        public void WriteVerbose(
            string msg,
            params object[] objToLog)
        {
            DateTime dt = DateTime.Now;
            StringBuilder sBuilder = new StringBuilder("[VERB][" + dt.ToLongTimeString() + "." + dt.Millisecond + "]");
            sBuilder.Append(MessageBuilder.FormatString(msg, objToLog));
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
        }


        /// 
        /// <summary>
        /// Starts a test (as a child of the current context)
        /// </summary>
        /// <param name="testId">Test id</param>
        ///         
        public void StartTest(
            string testId)
        {
            StringBuilder sBuilder = new StringBuilder("[START] Test: ");
            sBuilder.Append(testId);
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
        }

        /// 
        /// <summary>
        /// Ends the specified test with the specified test result
        /// </summary>
        /// <param name="testId">Test id</param>
        /// <param name="result">Result of the Test</param>
        /// 
        public void EndTest(
            string testId,
            TestResult result)
        {
            StringBuilder sBuilder = new StringBuilder("[END] Test: ");
            sBuilder.Append(testId);
            sBuilder.Append(" RESULT: ");
            sBuilder.Append(result.ToString());
            m_file.WriteLine(sBuilder.ToString());
            m_file.Flush();
            return;
        }

        
        /// 
        /// <summary>
        /// Returns "this" object
        /// </summary>
        /// <returns>SimpleFileLogger object</returns>
        /// 
        public object GetLogger()
        {
            return this;
        }

        /// 
        /// <summary>
        /// Closes the log file
        /// </summary>
        /// 
        public void Close()
        {
            if (m_file != null)
            {
                m_file.Flush();
#if DOTNET5_4
                m_file.Dispose();
#else
                m_file.Close();
#endif
            }
        }
    }
}
