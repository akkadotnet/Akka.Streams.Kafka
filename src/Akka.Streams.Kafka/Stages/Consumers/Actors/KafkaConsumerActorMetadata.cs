// -----------------------------------------------------------------------
//  <copyright file="KafkaConsumerActorMetadata.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using Decider = Akka.Streams.Supervision.Decider;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors;

/// <summary>
/// Contains metadata for <see cref="KafkaConsumerActor{K,V}"/>.
/// Generally this should not be used from outside the library.
/// </summary>
[InternalApi]
public static class KafkaConsumerActorMetadata
{
    private static readonly AtomicCounter Number = new(0);

    /// <summary>
    /// Gets next actor number in thread-safe way
    /// </summary>
    /// <returns></returns>
    public static int NextNumber() => Number.GetAndIncrement();

    public static Props GetProps<K, V>(ConsumerSettings<K, V> settings, Decider decider) =>
        GetProps(null, settings, decider, null);

    internal static Props GetProps<K, V>(IActorRef? owner, ConsumerSettings<K, V> settings, Decider decider,
        IStatisticsHandler? statisticsHandler) =>
        Props.Create(() => new KafkaConsumerActor<K, V>(owner, settings, decider,
            statisticsHandler ?? StatisticsHandlers.Empty.Instance)).WithDispatcher(settings.DispatcherId);


    /// <summary>
    /// Contains <see cref="KafkaConsumerActor{K,V}"/>  message definitions.
    /// Generally this should not be used from outside the library.
    /// </summary>
    [InternalApi]
    public static class Internal
    {
        /// <summary>
        /// Marker interface for subscription requests
        /// </summary>
        public interface ISubscriptionRequest : INoSerializationVerificationNeeded
        {
        }

        /* REQUESTS */

        /// <summary>
        /// Manual assignment of a partition - only used in conjunction with <see cref="IManualSubscription"/>
        /// </summary>
        public sealed record Assign(IImmutableSet<TopicPartition> TopicPartitions) : ISubscriptionRequest;

        /// <summary>
        /// Manual assignment of a partition with a specific offset - only used in conjunction
        /// with <see cref="IManualSubscription"/>
        /// </summary>
        public sealed record AssignWithOffset(IImmutableSet<TopicPartitionOffset> TopicPartitionOffsets)
            : ISubscriptionRequest;

        /// <summary>
        /// Subscribe to a set of topics.
        /// </summary>
        /// <param name="Topics">The topics to subscribe to.</param>
        /// <param name="RebalanceHandler">Optional - used to help handle and filter incoming rebalance events.</param>
        /// <param name="OffsetProvider">Optional - function to provide custom offsets for assigned partitions.</param>
        public sealed record Subscribe(
            IImmutableSet<string> Topics, 
            IPartitionEventHandler RebalanceHandler,
            Func<IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>>? OffsetProvider = null)
            : ISubscriptionRequest;

        /// <summary>
        /// Subscribe to topics fitting a specific pattern.
        /// </summary>
        /// <param name="TopicPattern">Topic pattern (regular expression to be matched)</param>
        /// <param name="RebalanceHandler">Optional - used to help handle and filter incoming rebalance events.</param>
        /// <param name="OffsetProvider">Optional - function to provide custom offsets for assigned partitions.</param>
        public sealed record SubscribePattern(
            string TopicPattern, 
            IPartitionEventHandler RebalanceHandler,
            Func<IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>>? OffsetProvider = null)
            : ISubscriptionRequest;

        /// <summary>
        /// Marker interface for shutdown messages to the KafkaConsumerActor
        /// </summary>
        public interface IStopLike : INoSerializationVerificationNeeded;

        /// <summary>
        /// Stops the consumer actor
        /// </summary>
        public sealed class Stop : IStopLike
        {
            public static readonly Stop Instance = new();

            private Stop()
            {
            }
        }

        internal sealed record StopFromStage(string StageId) : IStopLike;

        public sealed record RegisterSubStage(IImmutableSet<TopicPartition> TopicPartitions)
            : INoSerializationVerificationNeeded;

        public sealed record Seek(IImmutableSet<TopicPartitionOffset> Offsets) : INoSerializationVerificationNeeded;

        /// <summary>
        /// Request sent from a StageRef in a stream stage to the <see cref="KafkaConsumerActor{K,V}"/>
        /// for messages from a specific set of partitions.
        /// </summary>
        public sealed record RequestMessages(int RequestId, ImmutableHashSet<TopicPartition> Topics);

        internal interface ICommitLike
        {
            TopicPartitionOffset TopicPartitionOffset { get; }
        }

        /// <summary>
        /// Used to send commit requests to <see cref="KafkaConsumerActor{K,V}"/>
        /// </summary>
        /// <remarks>
        /// These belong to a batch commit.
        /// </remarks>
        public sealed record Commit(TopicPartition TopicPartition, OffsetAndMetadata OffsetAndMetadata)
            : INoSerializationVerificationNeeded, ICommitLike
        {
            public TopicPartitionOffset TopicPartitionOffset
            {
                get { return new TopicPartitionOffset(TopicPartition, OffsetAndMetadata.Offset); }
            }
        }

        public sealed record CommitWithoutReply(
            TopicPartition TopicPartition,
            OffsetAndMetadata OffsetAndMetadata,
            bool Emergency) : INoSerializationVerificationNeeded, ICommitLike
        {
            public TopicPartitionOffset TopicPartitionOffset
            {
                get { return new TopicPartitionOffset(TopicPartition, OffsetAndMetadata.Offset); }
            }
        }

        /// <summary>
        /// Execute a single commit without batching
        /// </summary>
        public sealed record CommitSingle(TopicPartition TopicPartition, OffsetAndMetadata OffsetAndMetadata)
            : INoSerializationVerificationNeeded, ICommitLike
        {
            public TopicPartitionOffset TopicPartitionOffset
            {
                get { return new TopicPartitionOffset(TopicPartition, OffsetAndMetadata.Offset); }
            }
        }

        /* RESPONSES */

        /// <summary>
        /// Messages from the Kafka consumer to be delivered back to the stream stage with the given <see cref="RequestId"/>.
        /// </summary>
        public sealed record Messages<K, V>(int RequestId, ImmutableList<ConsumeResult<K, V>> MessagesList)
            : INoSerializationVerificationNeeded;


        /// <summary>
        /// Collection of committed offsets
        /// </summary>
        public sealed record Committed(IImmutableSet<TopicPartitionOffset> Offsets)
            : INoSerializationVerificationNeeded;

        public sealed record Revoked(IImmutableSet<TopicPartition> Partitions) : INoSerializationVerificationNeeded;

        public sealed record Assigned(IImmutableSet<TopicPartition> Partitions)
            : INoSerializationVerificationNeeded;
    }
}