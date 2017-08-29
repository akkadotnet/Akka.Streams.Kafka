using System;
using Confluent.Kafka.Serialization;

namespace Akka.Streams.Kafka.Messages
{
    public sealed class MsgPackDeserializer<T> : ISerializer<T>, IDeserializer<T>
    {
        public byte[] Serialize(T data)
        {
            return MessagePack.MessagePackSerializer.Serialize(data);
        }

        public T Deserialize(byte[] data)
        {
            return MessagePack.MessagePackSerializer.Deserialize<T>(data);
        }
    }
}
