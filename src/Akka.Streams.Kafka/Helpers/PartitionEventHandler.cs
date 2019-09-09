using System;
using System.Collections.Immutable;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// This interface is used to pass callbacks when Kafka rebalances partitions between consumers,
    /// or an kafka consumer is stopped
    /// </summary>
    internal interface IPartitionEventHandler
    {
        /// <summary>
        /// Called when partitions are revoked
        /// </summary>
        void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions);

        /// <summary>
        /// Called when partitions are assigned
        /// </summary>
        void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions);

        /// <summary>
        /// Called when consuming is stopped
        /// </summary>
        void OnStop(IImmutableSet<TopicPartition> topicPartitions);
    }

    /// <summary>
    /// Dummy handler which does nothing. Also <see cref="IPartitionEventHandler"/>
    /// </summary>
    internal class EmptyPartitionEventHandler : IPartitionEventHandler
    {
        /// <inheritdoc />
        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions)
        {
        }

        /// <inheritdoc />
        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions)
        {
        }

        /// <inheritdoc />
        public void OnStop(IImmutableSet<TopicPartition> topicPartitions)
        {
        }
    }

    /// <summary>
    /// Handler allowing to pass custom stage callbacks. Also <see cref="IPartitionEventHandler"/>
    /// </summary>
    internal class AsyncCallbacksPartitionEventHandler : IPartitionEventHandler
    {
        private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
        private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;

        public AsyncCallbacksPartitionEventHandler(Action<IImmutableSet<TopicPartition>> partitionAssignedCallback,
                                                  Action<IImmutableSet<TopicPartitionOffset>> partitionRevokedCallback)
        {
            _partitionAssignedCallback = partitionAssignedCallback;
            _partitionRevokedCallback = partitionRevokedCallback;
        }

        /// <inheritdoc />
        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions)
        {
            _partitionRevokedCallback(revokedTopicPartitions);
        }

        /// <inheritdoc />
        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions)
        {
            _partitionAssignedCallback(assignedTopicPartitions);
        }

        /// <inheritdoc />
        public void OnStop(IImmutableSet<TopicPartition> topicPartitions)
        {
        }
    }
}