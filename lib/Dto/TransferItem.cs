using Microsoft.Azure.Storage.Blob;

namespace Microsoft.Azure.Storage.DataMovement.Dto
{
    /// <summary>
    /// Represents one transfer item that is combined of source path and destination information.
    /// </summary>
    public sealed class TransferItem
    {
        /// <summary>
        /// Default constructor for Transfer item.
        /// </summary>
        /// <param name="source">Source path.</param>
        /// <param name="destination">Destination blob.</param>
        public TransferItem(string source, string destination)
        {
            Source = source;
            Destination = destination;
        }
        
        /// <summary>
        /// Source path
        /// </summary>
        public string Source { get; private set; }
        
        /// <summary>
        /// Destination blob
        /// </summary>
        public string Destination { get; private set; }
    }
}