using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Offset position for a groupId, topic, partition.
    /// </summary>
    public record GroupTopicPartitionOffset
    {
        /// <summary>
        /// GroupTopicPartitionOffset
        /// </summary>
        public GroupTopicPartitionOffset(string groupId, string topic, int partition, Offset offset)
        {
            GroupId = groupId;
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        /// <summary>
        /// GroupTopicPartitionOffset
        /// </summary>
        public GroupTopicPartitionOffset(GroupTopicPartition groupTopicPartition, Offset offset)
            : this(groupTopicPartition.GroupId, groupTopicPartition.Topic, groupTopicPartition.Partition, offset)
        {
        }

        /// <summary>
        /// Consumer's group Id
        /// </summary>
        public string GroupId { get; }
        /// <summary>
        /// Topic
        /// </summary>
        public string Topic { get; }
        /// <summary>
        /// Partition
        /// </summary>
        public int Partition { get; }
        /// <summary>
        /// Kafka partition offset value
        /// </summary>
        public Offset Offset { get; }
        
        /// <summary>
        /// Group topic partition info
        /// </summary>
        public GroupTopicPartition GroupTopicPartition => new(GroupId, Topic, Partition);
    }
    
    /// <summary>
    /// Group, topic and partition info
    /// </summary>
    public sealed record GroupTopicPartition
    {
        public GroupTopicPartition(string groupId, string topic, int partition)
        {
            GroupId = groupId;
            Topic = topic;
            Partition = partition;
        }
        
        /// <summary>
        /// Consumer's group Id
        /// </summary>
        public string GroupId { get; }
        /// <summary>
        /// Topic
        /// </summary>
        public string Topic { get; }
        /// <summary>
        /// Partition
        /// </summary>
        public int Partition { get; }
    }

    public sealed record OffsetAndMetadata(Offset Offset, string Metadata)
    {
        /// <summary>
        /// Kafka partition offset value
        /// </summary>
        public Offset Offset { get; } = Offset;

        /// <summary>
        /// Metadata
        /// </summary>
        public string Metadata { get; } = Metadata;
    }
}