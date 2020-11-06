using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// Offers parts of <see cref="IConsumer{K,V}"/> API which becomes available to <see cref="IPartitionEventHandler"/>
    /// </summary>
    public interface IRestrictedConsumer
    {
        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Assignment"/>
        /// </summary>
        List<TopicPartition> Assignment { get; }

        /// <summary>
        /// Get the first offset for the given partitions.
        /// </summary>
        List<TopicPartitionOffset> BeginningOffsets(IEnumerable<TopicPartition> topicPartitions);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Commit()"/>
        /// </summary>
        void CommitSync(IEnumerable<TopicPartitionOffset> offsets);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Committed"/>
        /// </summary>
        void Committed(IEnumerable<TopicPartition> topicPartitions);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Position"/>
        /// </summary>
        Offset Position(TopicPartition topicPartition);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Seek"/>
        /// </summary>
        void Seek(TopicPartitionOffset topicPartitionOffset);
    }

    /// <summary>
    /// Offers parts of <see cref="IConsumer{K,V}"/> API which becomes available to <see cref="IPartitionEventHandler"/>
    /// </summary>
    internal class RestrictedConsumer<K, V> : IRestrictedConsumer
    {
        private readonly IConsumer<K, V> _consumer;
        private readonly TimeSpan _duration;

        /// <summary>
        /// RestrictedConsumer
        /// </summary>
        public RestrictedConsumer(IConsumer<K, V> consumer, TimeSpan duration)
        {
            _consumer = consumer;
            _duration = duration;
        }

        /// <inheritdoc />
        public List<TopicPartition> Assignment => _consumer.Assignment;

        /// <inheritdoc />
        public List<TopicPartitionOffset> BeginningOffsets(IEnumerable<TopicPartition> topicPartitions)
        {
            var timestamps = topicPartitions.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(0, TimestampType.NotAvailable))).ToList();
            return _consumer.OffsetsForTimes(timestamps, _duration);
        }
        
        /// <inheritdoc />
        public void CommitSync(IEnumerable<TopicPartitionOffset> offsets) => _consumer.Commit(offsets);

        /// <inheritdoc />
        public void Committed(IEnumerable<TopicPartition> topicPartitions) => _consumer.Committed(topicPartitions, _duration);

        /// <inheritdoc />
        public Offset Position(TopicPartition topicPartition) => _consumer.Position(topicPartition);

        /// <inheritdoc />
        public void Seek(TopicPartitionOffset topicPartitionOffset) => _consumer.Seek(topicPartitionOffset);

    }
}