using System;

namespace Microsoft.Azure.Storage.DataMovement.Client.Logger
{
    public class ConsoleLogger : IDataMovementLogger
    {
        public void Info(string message)
        {
            Console.WriteLine($"[INFO] {message}");
        }

        public void Warning(string message)
        {
            Console.WriteLine($"[WARNING] {message}");
        }
        
        public void Warning(Exception exception, string message)
        {
            Console.WriteLine($"[WARNING] {message}");
            Console.WriteLine($"[WARNING] {exception.Message}");
        }
        
        public void Error(string message)
        {
            Console.WriteLine($"[ERROR] {message}");
        }
        
        public void Error(Exception exception, string message)
        {
            Console.WriteLine($"[ERROR] {message}"); 
            Console.WriteLine($"[ERROR] {exception.Message}");
        }
    }
}