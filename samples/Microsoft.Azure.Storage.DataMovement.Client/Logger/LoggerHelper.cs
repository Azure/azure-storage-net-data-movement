using System;
using System.Text;

namespace Microsoft.Azure.Storage.DataMovement.Client.Logger
{
    internal static class LoggerHelper
    {
        public static void LogFailedOrSkipped(this TransferEventArgs e, string jobId, string type)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"(Job id: {jobId}) {type}");
            sb.AppendLine($"\tSource: {e.Source}");
            sb.AppendLine($"\tDestination: {e.Destination}");
            sb.AppendLine($"\tException: {e.Exception}");
            Console.WriteLine(sb);
        }

        public static void PrintResult(this TransferStatus result, string jobId)
        {
            var sb = new StringBuilder();

            sb.AppendLine();
            sb.AppendLine($"JobId: {jobId}");
            sb.AppendLine($"Total Bytes: {result.BytesTransferred}");
            sb.AppendLine($"Total Files Transferred: {result.NumberOfFilesTransferred}");
            sb.AppendLine($"Total Files Skipped: {result.NumberOfFilesSkipped}");
            sb.AppendLine($"Total Files Failed: {result.NumberOfFilesFailed}");

            Console.WriteLine(sb);
        }
    }
}