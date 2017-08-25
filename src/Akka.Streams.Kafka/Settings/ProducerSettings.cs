using System.Collections.Generic;
using Akka.Actor;
using Confluent.Kafka.Serialization;

namespace Akka.Streams.Kafka.Settings
{
    public class ProducerSettings<TKey, TValue>
    {
        public ProducerSettings(ActorSystem system, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer):
            this(new Dictionary<string, object>(), keySerializer, valueSerializer)
        {
        }

        public ProducerSettings(Dictionary<string, object> properties, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            Properties = properties;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
        }

        public Dictionary<string, object> Properties { get; }

        public ISerializer<TKey> KeySerializer { get; }

        public ISerializer<TValue> ValueSerializer { get; }

        public ProducerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers)
        {
            Properties["bootstrap.servers"] = bootstrapServers;
            return this;
        }
    }
}
