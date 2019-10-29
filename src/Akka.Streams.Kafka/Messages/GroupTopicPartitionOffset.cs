using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Offset position for a groupId, topic, partition.
    /// </summary>
    public class GroupTopicPartitionOffset : IEquatable<GroupTopicPartitionOffset>
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
        public GroupTopicPartition GroupTopicPartition => new GroupTopicPartition(GroupId, Topic, Partition);

        public bool Equals(GroupTopicPartitionOffset other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return GroupId == other.GroupId && Topic == other.Topic && Partition == other.Partition && Offset.Equals(other.Offset);
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is GroupTopicPartitionOffset other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (GroupId != null ? GroupId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Topic != null ? Topic.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Partition;
                hashCode = (hashCode * 397) ^ Offset.GetHashCode();
                return hashCode;
            }
        }
    }
    
    /// <summary>
    /// Group, topic and partition info
    /// </summary>
    public sealed class GroupTopicPartition : IEquatable<GroupTopicPartition>
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

        public bool Equals(GroupTopicPartition other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return GroupId == other.GroupId && Topic == other.Topic && Partition == other.Partition;
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is GroupTopicPartition other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (GroupId != null ? GroupId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Topic != null ? Topic.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Partition;
                return hashCode;
            }
        }
    }

    public sealed class OffsetAndMetadata : IEquatable<OffsetAndMetadata>
    {
        public OffsetAndMetadata(Offset offset, string metadata)
        {
            Offset = offset;
            Metadata = metadata;
        }

        /// <summary>
        /// Kafka partition offset value
        /// </summary>
        public Offset Offset { get; }
        /// <summary>
        /// Metadata
        /// </summary>
        public string Metadata { get; }

        public bool Equals(OffsetAndMetadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offset.Equals(other.Offset) && Metadata == other.Metadata;
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is OffsetAndMetadata other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return (Offset.GetHashCode() * 397) ^ (Metadata != null ? Metadata.GetHashCode() : 0);
            }
        }
    }
}