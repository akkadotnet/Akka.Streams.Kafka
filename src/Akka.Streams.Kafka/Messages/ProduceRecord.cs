namespace Akka.Streams.Kafka.Messages
{
    public struct ProduceRecord<TKey, TValue>
    {
        public ProduceRecord(string topic, TKey key, TValue value, int partitionId = -1)
        {
            Topic = topic;
            Key = key;
            Value = value;
            PartitionId = partitionId;
        }

        public string Topic { get; }

        public TKey Key { get; }

        public TValue Value { get; }

        public int PartitionId { get; }
    }
}
