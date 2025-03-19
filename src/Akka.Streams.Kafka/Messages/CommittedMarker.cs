using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Stages.Consumers;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Committed marker
    /// </summary>
    internal interface ICommittedMarker
    {
        /// <summary>
        /// Marks offsets as already committed
        /// </summary>
        Task Committed(IImmutableDictionary<TopicPartition, OffsetAndMetadata> offsets);

        /// <summary>
        /// Marks committing failure
        /// </summary>
        void Failed();
    }

    /// <summary>
    /// Used by <see cref="TransactionalMessageBuilder{K,V}"/>
    /// </summary>
    internal sealed record PartitionOffsetCommittedMarker
    {
        /// <summary>
        /// Committed marker
        /// </summary>
        public ICommittedMarker CommittedMarker { get; }

        public PartitionOffsetCommittedMarker(string groupId, string topic, int partition, Offset offset, ICommittedMarker committedMarker) 
            : this(new GroupTopicPartitionOffset(groupId, topic, partition, offset), committedMarker)
        {
            CommittedMarker = committedMarker;
        }

        public PartitionOffsetCommittedMarker(GroupTopicPartitionOffset groupTopicPartition, ICommittedMarker committedMarker)
        {
            CommittedMarker = committedMarker;
            GroupTopicPartitionOffset = groupTopicPartition;
        }
        
        public GroupTopicPartitionOffset GroupTopicPartitionOffset { get; }
        
        public string GroupId => GroupTopicPartitionOffset.GroupId;
        
        public string Topic => GroupTopicPartitionOffset.Topic;
        
        public int Partition => GroupTopicPartitionOffset.Partition;
        
        public Offset Offset => GroupTopicPartitionOffset.Offset;
        
        public GroupTopicPartition GroupTopicPartition => GroupTopicPartitionOffset.GroupTopicPartition;
    }
}