using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Concrete;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    public interface IMessageBuilder<K, V, out TMessage>
    {
        /// <summary>
        /// Creates downstream message
        /// </summary>
        /// <remarks>
        /// We pass consumer here, because there is no way to get consumer instance from
        /// some global configuration, like Alpakka does getting consumer actor ref
        /// </remarks>
        TMessage CreateMessage(ConsumeResult<K, V> record);
    }
    
    /// <summary>
    /// Message builder used for <see cref="PlainSourceStage{K,V}"/>
    /// </summary>
    public class PlainMessageBuilder<K, V> : IMessageBuilder<K, V, ConsumeResult<K, V>>
    {
        public ConsumeResult<K, V> CreateMessage(ConsumeResult<K, V> record) => record;
    }
    
    /// <summary>
    /// This base class used for different committable source message builders
    /// </summary>
    internal abstract class CommittableMessageBuilderBase<K, V> : IMessageBuilder<K, V, CommittableMessage<K, V>>
    {
        /// <summary>
        /// Committed object
        /// </summary>
        public abstract IInternalCommitter Committer { get; }
        /// <summary>
        /// Consumer group Id
        /// </summary>
        public abstract string GroupId { get; }
        /// <summary>
        /// Method for extracting string metadata from consumed record
        /// </summary>
        public abstract string MetadataFromRecord(ConsumeResult<K, V> record);

        /// <inheritdoc />
        public CommittableMessage<K, V> CreateMessage(ConsumeResult<K, V> record)
        {
            var offset = new PartitionOffset(GroupId, record.Topic, record.Partition, record.Offset);
            return new CommittableMessage<K, V>(record, new CommittableOffset(Committer, offset, MetadataFromRecord(record)));
        }
    }

    /// <summary>
    /// Message builder used by <see cref="CommittableSourceStage{K,V}"/>
    /// </summary>
    internal class CommittableSourceMessageBuilder<K, V> : CommittableMessageBuilderBase<K, V>
    {
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromRecord;
        
        /// <inheritdoc />
        public override IInternalCommitter Committer { get; }

        /// <inheritdoc />
        public override string GroupId { get; }

        /// <summary>
        /// CommittableSourceMessageBuilder
        /// </summary>
        public CommittableSourceMessageBuilder(IInternalCommitter committer, string groupId, Func<ConsumeResult<K, V>, string> metadataFromRecord)
        {
            Committer = committer;
            GroupId = groupId;
            _metadataFromRecord = metadataFromRecord;
        }
        
        /// <inheritdoc />
        public override string MetadataFromRecord(ConsumeResult<K, V> record) => _metadataFromRecord(record);
    }
}