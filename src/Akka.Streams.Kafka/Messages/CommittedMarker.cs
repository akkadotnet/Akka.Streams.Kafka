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
    internal sealed class PartitionOffsetCommittedMarker : GroupTopicPartitionOffset
    {
        /// <summary>
        /// Committed marker
        /// </summary>
        public ICommittedMarker CommittedMarker { get; }

        public PartitionOffsetCommittedMarker(string groupId, string topic, int partition, Offset offset, ICommittedMarker committedMarker) 
            : base(groupId, topic, partition, offset)
        {
            CommittedMarker = committedMarker;
        }

        public PartitionOffsetCommittedMarker(GroupTopicPartition groupTopicPartition, Offset offset, ICommittedMarker committedMarker) 
            : base(groupTopicPartition, offset)
        {
            CommittedMarker = committedMarker;
        }
    }
}