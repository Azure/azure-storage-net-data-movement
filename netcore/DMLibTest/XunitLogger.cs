using MS.Test.Common.MsTestLib;
using System;
using System.Text;
using Xunit.Abstractions;

namespace DMLibTest
{
    public class XunitLogger : ILogger
    {
        private readonly ITestOutputHelper output;

        public XunitLogger(ITestOutputHelper outputHelper)
        {
            output = outputHelper;
        }

        public void Close()
        { }

        public void EndTest(string testId, TestResult result)
        {
            output.WriteLine($"[END] Test: {testId} RESULT: {result.ToString()}");
        }

        public object GetLogger()
        {
            return this;
        }

        public void StartTest(string testId)
        {
            output.WriteLine($"[START] Test: {testId}");
        }

        public void WriteError(string msg, params object[] objToLog)
        {
            WriteMessage("ERROR", msg, objToLog);
        }

        public void WriteInfo(string msg, params object[] objToLog)
        {
            WriteMessage("INFO", msg, objToLog);
        }

        public void WriteVerbose(string msg, params object[] objToLog)
        {
            WriteMessage("VERB", msg, objToLog);
        }

        public void WriteWarning(string msg, params object[] objToLog)
        {
            WriteMessage("WARN", msg, objToLog);
        }

        private void WriteMessage(string prefix, string msg, params object[] objToLog)
        {
            DateTime dt = DateTime.Now;
            StringBuilder sBuilder = new StringBuilder($"[{prefix}][{dt.ToString()}.{dt.Millisecond}]");
            sBuilder.Append(MessageBuilder.FormatString(msg, objToLog));
            output.WriteLine(sBuilder.ToString());
        }
    }
}
