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
    public class RestrictedConsumer<K, V>
    {
        private readonly IConsumer<K, V> _consumer;
        private readonly TimeSpan _duration;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="duration"></param>
        public RestrictedConsumer(IConsumer<K, V> consumer, TimeSpan duration)
        {
            _consumer = consumer;
            _duration = duration;
        }

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Assignment"/>
        /// </summary>
        public List<TopicPartition> Assignment => _consumer.Assignment;

        /// <summary>
        /// Get the first offset for the given partitions.
        /// </summary>
        public List<TopicPartitionOffset> BeginningOffsets(IEnumerable<TopicPartition> topicPartitions)
        {
            var timestamps = topicPartitions.Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(0, TimestampType.NotAvailable))).ToList();
            return _consumer.OffsetsForTimes(timestamps, _duration);
        }
        
        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Commit()"/>
        /// </summary>
        public void CommitSync(IEnumerable<TopicPartitionOffset> offsets) => _consumer.Commit(offsets);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Committed"/>
        /// </summary>
        public void Committed(IEnumerable<TopicPartition> topicPartitions) => _consumer.Committed(topicPartitions, _duration);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Position"/>
        /// </summary>
        public Offset Position(TopicPartition topicPartition) => _consumer.Position(topicPartition);

        /// <summary>
        /// See <see cref="IConsumer{TKey,TValue}.Seek"/>
        /// </summary>
        public void Seek(TopicPartitionOffset topicPartitionOffset) => _consumer.Seek(topicPartitionOffset);

    }
}