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
        /// <summary>
        /// The message to send
        /// </summary>
        public Message<K, V> Message { get; set; }
        /// <summary>
        /// Topic to send to.
        /// </summary>
        /// <remarks>
        /// If TopicPartition property is specified, the Topic property value is ignored.
        /// </remarks>
        public string Topic { get; set; }
        /// <summary>
        /// Topic partition to sent to.
        /// </summary>
        /// <remarks>
        /// If TopicPartition property is specified, the Topic property value is ignored.
        /// </remarks>
        public TopicPartition TopicPartition { get; set; }
    }
}