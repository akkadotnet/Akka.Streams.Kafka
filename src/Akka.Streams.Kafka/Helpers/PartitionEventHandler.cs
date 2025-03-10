using System;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
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
        /// Called when partitions are lost
        /// </summary>
        void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer);

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
    /// Contains internal implementations of <see cref="IPartitionEventHandler"/>
    /// </summary>
    internal static class PartitionEventHandlers
    {
        /// <summary>
        /// Dummy handler which does nothing. Also <see cref="IPartitionEventHandler"/>
        /// </summary>
        internal sealed class Empty : IPartitionEventHandler
        {
            public static readonly Empty Instance = new();
            private Empty()
            {
                
            }
            
            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
            }

            /// <inheritdoc />
            public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
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
        /// Handler allowing to pass custom stage callbacks.
        /// </summary>
        internal class AsyncCallbacks : IPartitionEventHandler
        {
            private readonly IAutoSubscription _subscription;
            private readonly IActorRef _sourceActorRef;
            
            private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
            private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;
            private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionLostCallback;

            public AsyncCallbacks(IAutoSubscription subscription, 
                IActorRef sourceActorRef,
                Action<IImmutableSet<TopicPartition>> partitionAssignedCallback,
                Action<IImmutableSet<TopicPartitionOffset>> partitionRevokedCallback, 
                Action<IImmutableSet<TopicPartitionOffset>> partitionLostCallback)
            {
                _partitionAssignedCallback = partitionAssignedCallback;
                _partitionRevokedCallback = partitionRevokedCallback;
                _partitionLostCallback = partitionLostCallback;
                _subscription = subscription;
                _sourceActorRef = sourceActorRef;
            }

            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                // _subscription.RebalanceListener
                //     .OnSuccess(@ref =>
                //     {
                //         @ref.Tell(new KafkaConsumerActorMetadata.Internal.Revoked(revokedTopicPartitions));
                //     });
                
                _partitionRevokedCallback(revokedTopicPartitions);
            }

            /// <inheritdoc />
            public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                _partitionLostCallback(revokedTopicPartitions);
            }

            /// <inheritdoc />
            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
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
            public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                _handler1?.OnLost(revokedTopicPartitions, consumer);
                _handler2?.OnLost(revokedTopicPartitions, consumer);
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