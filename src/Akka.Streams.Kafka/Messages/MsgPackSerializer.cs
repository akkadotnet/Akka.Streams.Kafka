using System;
using Confluent.Kafka.Serialization;
using MessagePack.Resolvers;

namespace Akka.Streams.Kafka.Messages
{
    public sealed class MsgPackDeserializer<T> : ISerializer<T>, IDeserializer<T>
    {
        public byte[] Serialize(T data)
        {
            return MessagePack.MessagePackSerializer.Serialize(data, ContractlessStandardResolver.Instance);
        }

        public T Deserialize(byte[] data)
        {
            return MessagePack.MessagePackSerializer.Deserialize<T>(data, ContractlessStandardResolver.Instance);
        }
    }
}
