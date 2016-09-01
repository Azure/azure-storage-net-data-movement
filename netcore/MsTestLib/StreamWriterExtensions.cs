using System.IO;

namespace MS.Test.Common.MsTestLib
{
    internal static class StreamWriterExtensions
    {
        public static void Close(this StreamWriter writer)
        {
            writer.Dispose();
        }
    }
}