using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Offset position for a groupId, topic, partition.
    /// </summary>
    public readonly record struct GroupTopicPartitionOffset(GroupTopicPartition GroupTopicPartition, Offset Offset)
    {
        public GroupTopicPartitionOffset(string groupId, string topic, int partition, Offset offset)
            : this(new GroupTopicPartition(groupId, topic, partition), offset)
        {
        }

        /// <summary>
        /// Consumer's group Id
        /// </summary>
        public string GroupId => GroupTopicPartition.GroupId;
        
        /// <summary>
        /// Topic
        /// </summary>
        public string Topic => GroupTopicPartition.Topic;
        
        /// <summary>
        /// Partition
        /// </summary>
        public int Partition => GroupTopicPartition.Partition;
        
        /// <summary>
        /// Kafka partition offset value
        /// </summary>
        public Offset Offset { get; } = Offset;

        /// <summary>
        /// Group topic partition info
        /// </summary>
        public GroupTopicPartition GroupTopicPartition { get; } = GroupTopicPartition;
    }
    
    /// <summary>
    /// Group, topic and partition info
    /// </summary>
    public readonly record struct GroupTopicPartition(string GroupId, string Topic, int Partition)
    {
        public TopicPartition TopicPartition { get; } = new(Topic, Partition);
    }
    
    public readonly record struct OffsetAndMetadata(Offset Offset, string Metadata);
}