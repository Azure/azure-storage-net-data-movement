using System;

namespace Microsoft.Azure.Storage.DataMovement
{
    internal class NullLogger : IDataMovementLogger
    {
        private static readonly Lazy<NullLogger> _instance = new Lazy<NullLogger>(() => new NullLogger());

        public static NullLogger Instance => _instance.Value;

        private NullLogger()
        { }
        
        public void Dispose()
        { }

        public void Info(string message)
        { }

        public void Warning(string message)
        { }

        public void Warning(Exception exception, string message)
        { }

        public void Error(string message)
        { }

        public void Error(Exception exception, string message)
        { }
    }
}