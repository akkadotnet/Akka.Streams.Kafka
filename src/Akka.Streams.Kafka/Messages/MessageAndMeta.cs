using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Container for storing message and topic/parition info
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public class MessageAndMeta<K, V>
    {
        public Message<K, V> Message { get; set; }
        public string Topic { get; set; }
        public TopicPartition TopicPartition { get; set; }
    }
}