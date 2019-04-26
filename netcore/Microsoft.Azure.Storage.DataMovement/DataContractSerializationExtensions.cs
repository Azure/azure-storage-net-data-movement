using System.IO;
using System.Runtime.Serialization;

namespace Microsoft.Azure.Storage.DataMovement
{
    internal static class DataContractSerializationExtensions
    {
        public static void Serialize(this DataContractSerializer serializer, Stream stream, object graph)
        {
            serializer.WriteObject(stream, graph);
        }

        public static object Deserialize(this DataContractSerializer serializer, Stream stream)
        {
            return serializer.ReadObject(stream);
        }
    }
}
