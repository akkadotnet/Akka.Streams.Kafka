using System;
using System.Collections.Immutable;
using Akka.Annotations;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// The API is new and may change in further releases.
    ///
    /// Allows to execute user code when Kafka rebalances partitions between consumers, or an Alpakka Kafka consumer is stopped.
    /// Use with care: These callbacks are called synchronously on the same thread Kafka's `poll()` is called.
    /// A warning will be logged if a callback takes longer than the configured `partition-handler-warning`.
    ///
    /// There is no point in calling `CommittableOffset`'s commit methods as their committing won't be executed as long as any of
    /// the callbacks in this class are called.
    /// </summary>
    [ApiMayChange]
    public interface IPartitionEventHandler
    {
        /// <summary>
        /// Called when partitions are revoked
        /// </summary>
        void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer);

        /// <summary>
        /// Called when partitions are assigned
        /// </summary>
        void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer);

        /// <summary>
        /// Called when consuming is stopped
        /// </summary>
        void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer);
    }

    /// <summary>
    /// Contains internal imlementations of <see cref="IPartitionEventHandler"/>
    /// </summary>
    internal static class PartitionEventHandlers
    {
        /// <summary>
        /// Dummy handler which does nothing. Also <see cref="IPartitionEventHandler"/>
        /// </summary>
        internal class Empty : IPartitionEventHandler
        {
            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
            }

            /// <inheritdoc />
            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
            }

            /// <inheritdoc />
            public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
            {
            }
        }
        
        /// <summary>
        /// Handler allowing to pass custom stage callbacks. Also <see cref="IPartitionEventHandler{K,V}"/>
        /// </summary>
        internal class AsyncCallbacks : IPartitionEventHandler
        {
            private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
            private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;

            public AsyncCallbacks(Action<IImmutableSet<TopicPartition>> partitionAssignedCallback,
                Action<IImmutableSet<TopicPartitionOffset>> partitionRevokedCallback)
            {
                _partitionAssignedCallback = partitionAssignedCallback;
                _partitionRevokedCallback = partitionRevokedCallback;
            }

            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                _partitionRevokedCallback(revokedTopicPartitions);
            }

            /// <inheritdoc />
            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
                Console.WriteLine("AsyncPartitionCallback");
                _partitionAssignedCallback(assignedTopicPartitions);
            }

            /// <inheritdoc />
            public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
            {
            }
        }
        
        /// <summary>
        /// Handler allowing chain other implementations of <see cref="IPartitionEventHandler"/>
        /// </summary>
        internal class Chain : IPartitionEventHandler
        {
            private readonly IPartitionEventHandler _handler1;
            private readonly IPartitionEventHandler _handler2;

            public Chain(IPartitionEventHandler handler1, IPartitionEventHandler handler2)
            {
                _handler1 = handler1;
                _handler2 = handler2;
            }

            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                _handler1?.OnRevoke(revokedTopicPartitions, consumer);
                _handler2?.OnRevoke(revokedTopicPartitions, consumer);
            }

            /// <inheritdoc />
            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
                _handler1?.OnAssign(assignedTopicPartitions, consumer);
                _handler2?.OnAssign(assignedTopicPartitions, consumer);
            }

            /// <inheritdoc />
            public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
            {
                _handler1?.OnStop(topicPartitions, consumer);
                _handler2?.OnStop(topicPartitions, consumer);
            }
        }
    }
}