using Akka.Streams.Kafka.Dsl;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Output element of <see cref="KafkaConsumer.TransactionalSource{K,V}"/>
    /// The offset is automatically committed as by the Producer
    /// </summary>
    public sealed class TransactionalMessage<K, V>
    {
        /// <summary>
        /// TransactionalMessage
        /// </summary>
        public TransactionalMessage(ConsumeResult<K, V> record, GroupTopicPartitionOffset partitionOffset)
        {
            Record = record;
            PartitionOffset = partitionOffset;
        }

        /// <summary>
        /// Consumed record
        /// </summary>
        public ConsumeResult<K, V> Record { get; }
        /// <summary>
        /// Partition offset
        /// </summary>
        public GroupTopicPartitionOffset PartitionOffset { get; }
    }
}