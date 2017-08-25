using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    public struct Result<TKey, TValue>
    {
        public Result(Message<TKey, TValue> metadata, ProduceRecord<TKey, TValue> message)
        {
            Metadata = metadata;
            Message = message;
        }

        public Message<TKey, TValue> Metadata { get; }

        public ProduceRecord<TKey, TValue> Message { get; }

        public Offset Offset => Metadata.Offset;
    }
}