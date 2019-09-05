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
        public abstract IInternalCommitter Committer { get; }
        public abstract string GroupId { get; }
        public abstract string MetadataFromRecord(ConsumeResult<K, V> record);

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
        private readonly IInternalCommitter _committer;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromRecord;

        public CommittableSourceMessageBuilder(IInternalCommitter committer, ConsumerSettings<K, V> settings, Func<ConsumeResult<K, V>, string> metadataFromRecord)
        {
            _committer = committer;
            _settings = settings;
            _metadataFromRecord = metadataFromRecord;
        }

        public override IInternalCommitter Committer => _committer;
        public override string GroupId => _settings.GroupId;
        public override string MetadataFromRecord(ConsumeResult<K, V> record) => _metadataFromRecord(record);
    }
}