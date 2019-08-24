using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Stages.Consumers;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Output element of <see cref="KafkaConsumer.CommittableSource{K,V}"/>.
    /// The offset can be committed via the included <see cref="CommitableOffset"/>.
    /// </summary>
    public sealed class CommittableMessage<K, V>
    {
        public CommittableMessage(ConsumeResult<K, V> record, CommitableOffset commitableOffset)
        {
            Record = record;
            CommitableOffset = commitableOffset;
        }

        public ConsumeResult<K, V> Record { get; }
        public CommitableOffset CommitableOffset { get; }
    }

    /// <summary>
    /// Commit an offset that is included in a <see cref="CommittableMessage{K,V}"/>
    /// If you need to store offsets in anything other than Kafka, this API
    /// should not be used.
    /// </summary>
    public interface ICommittable
    {
        Task Commit();
    }

    /// <summary>
    /// Included in <see cref="CommittableMessage{K,V}"/>. Makes it possible to
    /// commit an offset or aggregate several offsets before committing.
    /// Note that the offset position that is committed to Kafka will automatically
    /// be one more than the `offset` of the message, because the committed offset
    /// should be the next message your application will consume,
    /// i.e. lastProcessedMessageOffset + 1.
    /// </summary>
    public interface ICommittableOffset : ICommittable
    {
        PartitionOffset Offset { get; }
    }

    public interface ICommittableOffsetMetadata : ICommittableOffset
    {
        string Metadata { get; }
    }

    
    public class CommitableOffset : ICommittableOffsetMetadata
    {
        private readonly IInternalCommitter _committer;
        public PartitionOffset Offset { get; }
        public string Metadata { get; }

        public CommitableOffset(IInternalCommitter committer, PartitionOffset offset, string metadata)
        {
            _committer = committer;
            Offset = offset;
            Metadata = metadata;
        }

        public Task Commit()
        {
            return Task.FromResult(_committer.Commit());
        }
    }

    /// <summary>
    /// Offset position for a groupId, topic, partition.
    /// </summary>
    public class PartitionOffset
    {
        public PartitionOffset(string groupId, string topic, int partition, Offset offset)
        {
            GroupId = groupId;
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        public string GroupId { get; }

        public string Topic { get; }

        public int Partition { get; }

        public Offset Offset { get; }
    }
}
