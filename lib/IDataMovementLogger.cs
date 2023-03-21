using System;

namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// DataMovement logger interface.
    /// </summary>
    public interface IDataMovementLogger
    {
        /// <summary>
        /// Info level logging.
        /// </summary>
        /// <param name="message"></param>
        void Info(string message);


        /// <summary>
        /// Warning level logging.
        /// </summary>
        /// <param name="message"></param>
        void Warning(string message);
        
        /// <summary>
        /// Warning level logging.
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        void Warning(Exception exception, string message);
        
        /// <summary>
        /// Error level logging.
        /// </summary>
        /// <param name="message"></param>
        void Error(string message);
        
        /// <summary>
        /// Error level logging.
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        void Error(Exception exception, string message);
    }
}