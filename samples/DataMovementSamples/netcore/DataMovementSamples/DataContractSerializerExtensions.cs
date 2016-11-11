using System.IO;
using System.Runtime.Serialization;

namespace DataMovementSamples
{
    internal static class DataContractSerializerExtensions
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
