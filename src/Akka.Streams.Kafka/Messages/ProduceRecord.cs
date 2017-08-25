namespace Akka.Streams.Kafka.Messages
{
    public struct ProduceRecord<TKey, TValue>
    {
        public ProduceRecord(string topic, TKey key, TValue value)
        {
            Topic = topic;
            Key = key;
            Value = value;
        }

        public string Topic { get; }

        public TKey Key { get; }

        public TValue Value { get; }
    }
}
