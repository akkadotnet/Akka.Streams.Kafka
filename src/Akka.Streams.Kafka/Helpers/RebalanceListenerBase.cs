using System;
using System.Collections.Immutable;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// Concrete implementations of this base class are used by <see cref="KafkaConsumerActor{K,V}"/> to
    /// wrap <see cref="IPartitionEventHandler"/> invocations
    /// </summary>
    internal abstract class RebalanceListenerBase
    {
        /// <summary>
        /// Called on partitions assigned
        /// </summary>
        public abstract void OnPartitionsAssigned(IImmutableSet<TopicPartition> partitions);
        /// <summary>
        /// Called on partitions revoked
        /// </summary>
        public abstract void OnPartitionsRevoked(IImmutableSet<TopicPartitionOffset> partitions);

        /// <summary>
        /// Called on stop consuming
        /// </summary>
        public virtual void PostStop()
        {
        }
    }
}