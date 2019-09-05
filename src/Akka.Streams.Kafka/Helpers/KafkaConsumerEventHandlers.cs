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
    public interface IConsumerEventHandler
    {
        /// <summary>
        /// Called when consumer error occures
        /// </summary>
        void OnError(Error error);
        
        /// <summary>
        /// Called when partitions are revoked
        /// </summary>
        void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions);

        /// <summary>
        /// Called when partitions are assigned
        /// </summary>
        /// <param name="assignedTopicPartitions"></param>
        void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions);
    }

    /// <summary>
    /// Dummy handler which does nothing. Also <see cref="IConsumerEventHandler"/>
    /// </summary>
    internal class EmptyConsumerEventHandler : IConsumerEventHandler
    {
        /// <inheritdoc />
        public void OnError(Error error)
        {
        }

        /// <inheritdoc />
        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions)
        {
        }

        /// <inheritdoc />
        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions)
        {
        }
    }

    /// <summary>
    /// Handler allowing to pass custom stage callbacks. Also <see cref="IConsumerEventHandler"/>
    /// </summary>
    internal class AsyncCallbacksConsumerEventHandler : IConsumerEventHandler
    {
        private readonly Action<Error> _errorCallback;
        private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
        private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;

        public AsyncCallbacksConsumerEventHandler(Action<Error> errorCallback,
                                                  Action<IImmutableSet<TopicPartition>> partitionAssignedCallback,
                                                  Action<IImmutableSet<TopicPartitionOffset>> partitionRevokedCallback)
        {
            _errorCallback = errorCallback;
            _partitionAssignedCallback = partitionAssignedCallback;
            _partitionRevokedCallback = partitionRevokedCallback;
        }

        /// <inheritdoc />
        public void OnError(Error error)
        {
            _errorCallback(error);
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
    }
}