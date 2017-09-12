using System;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using MessagePack.Resolvers;

namespace Akka.Streams.Kafka.Messages
{
    public sealed class MsgPackDeserializer<T> : ISerializer<T>, IDeserializer<T>
    {
        public byte[] Serialize(string topic, T data)
        {
            return MessagePack.MessagePackSerializer.Serialize(data, ContractlessStandardResolver.Instance);
        }

        public T Deserialize(string topic, byte[] data)
        {
            return MessagePack.MessagePackSerializer.Deserialize<T>(data, ContractlessStandardResolver.Instance);
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
