using System;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// This is used by <see cref="KafkaConsumerActor{K,V}"/> to store consumer offsets
    /// </summary>
    internal interface ICommitRefreshing<K, V>
    {
        void Add(IImmutableSet<TopicPartitionOffset> offsets);
        void Committed(IImmutableSet<TopicPartitionOffset> offsets);
        void Revoke(IImmutableSet<TopicPartition> revokedTopicPartitions);
        IImmutableSet<TopicPartitionOffset> RefreshOffsets { get; }
        void UpdateRefreshDeadlines(IImmutableSet<TopicPartition> topicPartitions);
        void AssignedPositions(IImmutableSet<TopicPartition> assignedPartitions, IImmutableSet<TopicPartitionOffset> assignedOffsets);
        void AssignedPositions(IImmutableSet<TopicPartition> assignedTopicPartitions, IConsumer<K, V> consumer, TimeSpan positionTimeout);
    }

    internal static class CommitRefreshing
    {
        public static ICommitRefreshing<K, V> Create<K, V>(TimeSpan commitRefreshInterval)
        {
            if (commitRefreshInterval == TimeSpan.MaxValue)
                return new NoOp<K, V>();
            else
                return new Impl<K, V>(commitRefreshInterval);
                
        }

        public class NoOp<K, V> : ICommitRefreshing<K, V>
        {
            public void Add(IImmutableSet<TopicPartitionOffset> offsets) { }
            public void Committed(IImmutableSet<TopicPartitionOffset> offsets)  { }
            public void Revoke(IImmutableSet<TopicPartition> revokedTopicPartitions)  { }
            public IImmutableSet<TopicPartitionOffset> RefreshOffsets => ImmutableHashSet<TopicPartitionOffset>.Empty;
            public void UpdateRefreshDeadlines(IImmutableSet<TopicPartition> topicPartitions)  { }
            public void AssignedPositions(IImmutableSet<TopicPartition> assignedPartitions, IImmutableSet<TopicPartitionOffset> assignedOffsets)  { }
            public void AssignedPositions(IImmutableSet<TopicPartition> assignedTopicPartitions, IConsumer<K, V> consumer, TimeSpan positionTimeout)  { }
        }

        public class Impl<K, V> : ICommitRefreshing<K, V>
        {
            private readonly TimeSpan _commitRefreshInterval;
            private IImmutableSet<TopicPartitionOffset> _requestedOffsets = ImmutableHashSet<TopicPartitionOffset>.Empty;
            private IImmutableSet<TopicPartitionOffset> _committedOffsets = ImmutableHashSet<TopicPartitionOffset>.Empty;
            private IImmutableDictionary<TopicPartition, DateTime> _refreshDeadlines = ImmutableDictionary<TopicPartition, DateTime>.Empty;

            public Impl(TimeSpan commitRefreshInterval)
            {
                _commitRefreshInterval = commitRefreshInterval;
            }
            
            public void Add(IImmutableSet<TopicPartitionOffset> offsets) => _requestedOffsets = _requestedOffsets.Union(offsets);

            public void Committed(IImmutableSet<TopicPartitionOffset> offsets) => _committedOffsets = _committedOffsets.Union(offsets);

            public void Revoke(IImmutableSet<TopicPartition> revokedTopicPartitions)
            {
                _requestedOffsets = _requestedOffsets.Where(tp => revokedTopicPartitions.Contains(tp.TopicPartition)).ToImmutableHashSet();
                _committedOffsets = _committedOffsets.Where(tp => revokedTopicPartitions.Contains(tp.TopicPartition)).ToImmutableHashSet();
                _refreshDeadlines.RemoveRange(revokedTopicPartitions);
            }

            public IImmutableSet<TopicPartitionOffset> RefreshOffsets
            {
                get
                {
                    var overdueTopicPartitions = _refreshDeadlines.Where(deadline => deadline.Value < DateTime.UtcNow)
                        .Select(d => d.Key)
                        .ToImmutableHashSet();

                    if (overdueTopicPartitions.Any())
                    {
                        return _committedOffsets.Where(topicPartitionOffset => overdueTopicPartitions.Contains(topicPartitionOffset.TopicPartition) && 
                                                                               _requestedOffsets.Contains(topicPartitionOffset)).ToImmutableHashSet();
                    }
                    else
                    {
                        return ImmutableHashSet<TopicPartitionOffset>.Empty;
                    }
                }
            }

            public void UpdateRefreshDeadlines(IImmutableSet<TopicPartition> topicPartitions)
            {
                _refreshDeadlines = _refreshDeadlines.SetItems(topicPartitions.ToImmutableDictionary(p => p, p => DateTime.UtcNow.Add(_commitRefreshInterval)));
            }

            public void AssignedPositions(IImmutableSet<TopicPartition> assignedPartitions, IImmutableSet<TopicPartitionOffset> assignedOffsets)
            {
                var requestedOffsetsToAdd = assignedOffsets.Where(offset => !_requestedOffsets.Contains(offset));
                _requestedOffsets = _requestedOffsets.Union(requestedOffsetsToAdd);
                
                var committedOffsetsToAdd = assignedOffsets.Where(offset => !_committedOffsets.Contains(offset));
                _requestedOffsets = _committedOffsets.Union(committedOffsetsToAdd);
                
                UpdateRefreshDeadlines(assignedPartitions);
            }

            public void AssignedPositions(IImmutableSet<TopicPartition> assignedTopicPartitions,
                                          IConsumer<K, V> consumer, TimeSpan positionTimeout)
            {
                var assignedOffsets = assignedTopicPartitions.Select(tp =>
                {
                    var offset = consumer.Position(tp);
                    return new TopicPartitionOffset(tp, offset);
                }).ToImmutableHashSet();
                
                AssignedPositions(assignedTopicPartitions, assignedOffsets);
            }
        }
    }
}