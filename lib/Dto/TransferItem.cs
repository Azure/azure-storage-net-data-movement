using Microsoft.Azure.Storage.Blob;

namespace Microsoft.Azure.Storage.DataMovement.Dto
{
    // <summary> Represents one transfer item that is combined of source path and destination information.</summary>
    public sealed class TransferItem
    {
        public TransferItem(string source, IListBlobItem destination)
        {
            Source = source;
            Destination = destination;
        }
        
        // <summary> Source path </summary>
        public string Source { get; private set; }
        
        // <summary> Destination blob </summary>
        public IListBlobItem Destination { get; private set; }
    }
}