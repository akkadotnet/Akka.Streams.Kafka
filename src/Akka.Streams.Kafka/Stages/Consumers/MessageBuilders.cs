using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
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
        /// We pass consumer group metadata here, because Confluent driver requires it instead of GroupId
        /// </remarks>
        TMessage CreateMessage(ConsumeResult<K, V> record, IConsumerGroupMetadata consumerGroupMetadata);
    }
    
    /// <summary>
    /// Message builder used for <see cref="PlainSourceStage{K,V}"/>
    /// </summary>
    public class PlainMessageBuilder<K, V> : IMessageBuilder<K, V, ConsumeResult<K, V>>
    {
        public ConsumeResult<K, V> CreateMessage(ConsumeResult<K, V> record, IConsumerGroupMetadata consumerGroupMetadata) => record;
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
        public CommittableMessage<K, V> CreateMessage(ConsumeResult<K, V> record, IConsumerGroupMetadata consumerGroupMetadata)
        {
            var offset = new GroupTopicPartitionOffset(GroupId, record.Topic, record.Partition, record.Offset);
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

    /// <summary>
    /// Message builder used by <see cref="SourceWithOffsetContextStage{K,V}"/>
    /// </summary>
    internal class OffsetContextBuilder<K, V> : IMessageBuilder<K, V, (ConsumeResult<K, V>, ICommittableOffset)>
    {
        /// <summary>
        /// Method for extracting string metadata from consumed record
        /// </summary>
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromMessage;
        /// <summary>
        /// Committed object
        /// </summary>
        public IInternalCommitter Committer { get; }
        /// <summary>
        /// Consumer group Id
        /// </summary>
        public string GroupId { get; }
        
        /// <summary>
        /// OffsetContextBuilder
        /// </summary>
        public OffsetContextBuilder(IInternalCommitter committer, ConsumerSettings<K, V> setting, Func<ConsumeResult<K, V>, string> metadataFromMessage)
        {
            _metadataFromMessage = metadataFromMessage;
            Committer = committer;
            GroupId = setting.GroupId;
        }

        /// <inheritdoc />
        public (ConsumeResult<K, V>, ICommittableOffset) CreateMessage(
            ConsumeResult<K, V> record, 
            IConsumerGroupMetadata consumerGroupMetadata)
        {
            var offset = new GroupTopicPartitionOffset(GroupId, record.Topic, record.Partition, record.Offset);
            return (record, new CommittableOffset(Committer, offset, _metadataFromMessage(record)));
        }
    }

    /// <summary>
    /// Base interface for transactional message builders
    /// </summary>
    internal interface ITransactionalMessageBuilderStage<K, V, TMsg> : IMessageBuilder<K, V, TMsg>
    {
        /// <summary>
        /// Consumer's group Id
        /// </summary>
        string GroupId { get; }
        /// <summary>
        /// Committed marker for consumed offset
        /// </summary>
        ICommittedMarker CommittedMarker { get; }
        /// <summary>
        /// Consumer group metadata
        /// </summary>
        IConsumerGroupMetadata ConsumerGroupMetadata { get; }
        /// <summary>
        /// On message callback
        /// </summary>
        /// <param name="message"></param>
        void OnMessage(ConsumeResult<K, V> message);
    }

    /// <summary>
    /// Message builder used by <see cref="TransactionalSourceStage{K,V}"/>
    /// </summary>
    internal class TransactionalMessageBuilder<K, V> : IMessageBuilder<K, V, TransactionalMessage<K, V>>
    {
        private readonly ITransactionalMessageBuilderStage<K, V, TransactionalMessage<K, V>> _transactionalMessageBuilderStage;

        public TransactionalMessageBuilder(ITransactionalMessageBuilderStage<K, V, TransactionalMessage<K, V>> transactionalMessageBuilderStage)
        {
            _transactionalMessageBuilderStage = transactionalMessageBuilderStage;
        }

        /// <inheritdoc />
        public TransactionalMessage<K, V> CreateMessage(
            ConsumeResult<K, V> record, 
            IConsumerGroupMetadata consumerGroupMetadata)
        {
            _transactionalMessageBuilderStage.OnMessage(record);
            
            var offset = new PartitionOffsetCommittedMarker(
                _transactionalMessageBuilderStage.GroupId, 
                record.Topic, 
                record.Partition, 
                record.Offset, 
                _transactionalMessageBuilderStage.CommittedMarker,
                consumerGroupMetadata);
            
            return new TransactionalMessage<K, V>(record, offset);
        }
    }
}