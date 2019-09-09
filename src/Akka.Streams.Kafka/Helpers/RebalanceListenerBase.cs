using System;
using System.Collections.Immutable;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    internal abstract class RebalanceListenerBase
    {
        public abstract void OnPartitionsAssigned(IImmutableSet<TopicPartition> partitions);
        public abstract void OnPartitionsRevoked(IImmutableSet<TopicPartitionOffset> partitions);

        public virtual void PostStop()
        {
        }
    }
}