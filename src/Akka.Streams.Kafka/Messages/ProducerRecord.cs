using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// <para>
    /// A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
    /// partition number, and an optional key and value.
    /// </para>
    /// 
    /// <para>
    /// If a valid partition number is specified that partition will be used when sending the record. If no partition is
    /// specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
    /// present a partition will be assigned in a round-robin fashion.
    /// </para>
    ///
    /// <para>
    /// The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
    /// record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
    /// the topic:
    /// <list type="bullet">
    /// <item><description>
    /// If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
    /// the timestamp in the producer record will be used by the broker.
    /// </description></item>
    /// <item><description>
    /// If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
    /// the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
    /// message to its log.
    /// </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// In either of the cases above, the timestamp that has actually been used will be returned to user in <see cref="RecordMetadata"/>
    /// </para>
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public class ProducerRecord<K, V> : IEquatable<ProducerRecord<K, V>>
    {
        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(string topic, int? partition, long? timestamp, Message<K, V> message)
        {
            if (topic == null)
                throw new ArgumentNullException(nameof(topic), "Topic cannot be null");
            if (timestamp != null && timestamp < 0)
                throw new ArgumentException($"Invalid timestamp: {timestamp}. Timestamp should always be non-negative or null.");
            if (partition != null && partition < 0)
                throw new ArgumentException($"Invalid partition: {partition}. Partition number should always be non-negative or null.");
            
            Topic = topic;
            Partition = partition;
            Timestamp = timestamp;
            Message = message;
        }

        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(string topic, int? partition, Message<K, V> message)
            : this(topic, partition, null, message)
        {
        }
        
        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(TopicPartition topicPartition, Message<K, V> message)
            : this(topicPartition.Topic, topicPartition.Partition, null, message)
        {
        }
        
        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(string topic, Message<K, V> message)
            : this(topic, null, null, message)
        {
        }
        
        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(string topic, V value)
            : this(topic, null, null, new Message<K, V>(){ Value = value })
        {
        }
        
        /// <summary>
        /// ProducerRecord
        /// </summary>
        public ProducerRecord(TopicPartition topicPartition, V value)
            : this(topicPartition.Topic, topicPartition.Partition, null, new Message<K, V>(){ Value = value })
        {
        }

        /// <summary>
        /// Topic to send to.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// Partition to sent to.
        /// </summary>
        public int? Partition { get; set; }
        /// <summary>
        /// Timestamp
        /// </summary>
        public long? Timestamp { get; set; }
        /// <summary>
        /// The message to send
        /// </summary>
        public Message<K, V> Message { get; set; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"ProducerRecord(" +
                       $"topic={Topic}, " +
                       $"partition={Partition?.ToString() ?? "null"}, " +
                       $"key={Message?.Key?.ToString() ?? "null"}, " +
                       $"value={Message?.Value?.ToString() ?? "null"}, " +
                       $"timestamp={Timestamp?.ToString() ?? "null"}" +
                   $")";
        }

        public bool Equals(ProducerRecord<K, V> other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topic == other.Topic && Partition == other.Partition && Timestamp == other.Timestamp && Equals(Message, other.Message);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ProducerRecord<K, V>) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Topic != null ? Topic.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Partition.GetHashCode();
                hashCode = (hashCode * 397) ^ Timestamp.GetHashCode();
                hashCode = (hashCode * 397) ^ (Message != null ? Message.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}