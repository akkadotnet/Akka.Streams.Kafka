using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        public CommittableMessage(ConsumeResult<K, V> record, ICommittableOffset commitableOffset)
        {
            Record = record;
            CommitableOffset = commitableOffset;
        }

        /// <summary>
        /// The consumed record data
        /// </summary>
        public ConsumeResult<K, V> Record { get; }
        /// <summary>
        /// Consumer offset that can be commited
        /// </summary>
        public ICommittableOffset CommitableOffset { get; }
    }

    /// <summary>
    /// Commit an offset that is included in a <see cref="CommittableMessage{K,V}"/>
    /// If you need to store offsets in anything other than Kafka, this API
    /// should not be used.
    /// </summary>
    public interface ICommittable
    {
        /// <summary>
        /// Commits an offset that is included in a <see cref="CommittableMessage{K,V}"/> 
        /// </summary>
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
        /// <summary>
        /// Offset value
        /// </summary>
        PartitionOffset Offset { get; }
    }

    /// <summary>
    /// Extends <see cref="ICommittableOffset"/> with some metadata
    /// </summary>
    public interface ICommittableOffsetMetadata : ICommittableOffset
    {
        /// <summary>
        /// Cosumed record metadata
        /// </summary>
        string Metadata { get; }
    }

    /// <summary>
    /// Implementation of the offset, contained in <see cref="CommittableMessage{K,V}"/>.
    /// Can be commited via <see cref="Commit"/> method.
    /// </summary>
    internal class CommittableOffset : ICommittableOffsetMetadata
    {
        private readonly IInternalCommitter _committer;
        
        /// <summary>
        /// Offset value
        /// </summary>
        public PartitionOffset Offset { get; }
        /// <summary>
        /// Cosumed record metadata
        /// </summary>
        public string Metadata { get; }

        public CommittableOffset(IInternalCommitter committer, PartitionOffset offset, string metadata)
        {
            _committer = committer;
            Offset = offset;
            Metadata = metadata;
        }

        /// <summary>
        /// Commits offset to Kafka
        /// </summary>
        public Task Commit()
        {
            return _committer.Commit(new List<PartitionOffset>() { Offset }.ToImmutableList());
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
    }
}
