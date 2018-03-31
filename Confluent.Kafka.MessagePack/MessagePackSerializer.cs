using System.Collections.Generic;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka.MessagePack
{
    public sealed class MessagePackSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(string topic, T data)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            throw new System.NotImplementedException();
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }
    }
}
