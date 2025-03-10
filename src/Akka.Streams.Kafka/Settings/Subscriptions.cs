using System.Collections.Immutable;
using Akka.Actor;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Util;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Settings
{
    public interface ISubscription 
    {
        /// <summary>
        /// Statistics handler
        /// </summary>
        Option<IStatisticsHandler> StatisticsHandler { get; }

        /// <summary>
        /// Allows to specify custom statistics handler. See more at <see cref="IStatisticsHandler"/>
        /// </summary>
        ISubscription WithStatisticsHandler(IStatisticsHandler statisticsHandler);
    }

    public interface IManualSubscription : ISubscription { }

    public interface IAutoSubscription : ISubscription
    {
        /// <summary>
        /// Optional. Partition events handler.
        /// </summary>
        Option<IPartitionEventHandler> PartitionEventsHandler { get; }
        
        /// <summary>
        /// Optional actor that receives rebalance events as messages.
        /// </summary>
        Option<IActorRef> RebalanceListener { get; }
        
        /// <summary>
        /// Allows to specify custom partition events handler. See more at <see cref="IPartitionEventHandler"/>
        /// </summary>
        IAutoSubscription WithPartitionEventsHandler(IPartitionEventHandler partitionEventHandler);
        
        /// <summary>
        /// Specifies actor that receives rebalance events as messages.
        /// </summary>
        IAutoSubscription WithRebalanceListener(IActorRef rebalanceListener);
    }

    /// <summary>
    /// A subscription to a set of 1 or more topics.
    /// </summary>
    internal sealed record TopicSubscription : IAutoSubscription
    {
        /// <summary>
        /// TopicSubscription
        /// </summary>
        /// <param name="topics">List of topics to subscribe</param>
        public TopicSubscription(IImmutableSet<string> topics)
        {
            Topics = topics;
        }

        /// <summary>
        /// List of topics to subscribe
        /// </summary>
        public IImmutableSet<string> Topics { get; private init; }
        
        public Option<IStatisticsHandler> StatisticsHandler { get; private init; } = Option<IStatisticsHandler>.None;
        
        public ISubscription WithStatisticsHandler(IStatisticsHandler statisticsHandler)
        {
            var s = Option<IStatisticsHandler>.Create(statisticsHandler);
            return this with { StatisticsHandler = s };
        }
        
        public Option<IPartitionEventHandler> PartitionEventsHandler { get; private init; } = Option<IPartitionEventHandler>.None;

        public Option<IActorRef> RebalanceListener { get; private init; } = Option<IActorRef>.None;
        
        public IAutoSubscription WithPartitionEventsHandler(IPartitionEventHandler partitionEventHandler)
        {
            var p = Option<IPartitionEventHandler>.Create(partitionEventHandler);
            return this with { PartitionEventsHandler = p };
        }

        public IAutoSubscription WithRebalanceListener(IActorRef rebalanceListener)
        {
            return this with { RebalanceListener = Option<IActorRef>.Create(rebalanceListener) };
        }
    }
    
    /// <summary>
    /// TopicSubscriptionPattern
    /// </summary>
    /// <remarks>
    /// Allows subscription to multiple topics, matching given regex pattern
    /// </remarks>
    internal sealed record TopicSubscriptionPattern : IAutoSubscription
    {
        /// <summary>
        /// TopicSubscriptionPattern
        /// </summary>
        /// <param name="topicPattern">Topic pattern (regular expression to be matched)</param>
        public TopicSubscriptionPattern(string topicPattern)
        {
            TopicPattern = topicPattern;
        }

        /// <summary>
        /// Topic pattern (regular expression to be matched)
        /// </summary>
        public string TopicPattern { get; private init; }
        
        public Option<IStatisticsHandler> StatisticsHandler { get; private init; } = Option<IStatisticsHandler>.None;
        
        public ISubscription WithStatisticsHandler(IStatisticsHandler statisticsHandler)
        {
            var s = Option<IStatisticsHandler>.Create(statisticsHandler);
            return this with { StatisticsHandler = s };
        }
        
        public Option<IPartitionEventHandler> PartitionEventsHandler { get; private init; } = Option<IPartitionEventHandler>.None;

        public Option<IActorRef> RebalanceListener { get; private init; } = Option<IActorRef>.None;
        
        public IAutoSubscription WithPartitionEventsHandler(IPartitionEventHandler partitionEventHandler)
        {
            var p = Option<IPartitionEventHandler>.Create(partitionEventHandler);
            return this with { PartitionEventsHandler = p };
        }

        public IAutoSubscription WithRebalanceListener(IActorRef rebalanceListener)
        {
            return this with { RebalanceListener = Option<IActorRef>.Create(rebalanceListener) };
        }
    }

    /// <summary>
    /// Assignment subscription
    /// </summary>
    /// <remarks>
    /// Allows to subscribe to fixed set of topic partitions
    /// </remarks>
    internal sealed class Assignment : IManualSubscription
    {
        /// <summary>
        /// Assignment
        /// </summary>
        /// <param name="topicPartitions">List of topic partitions to subscribe</param>
        public Assignment(IImmutableSet<TopicPartition> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        /// <summary>
        /// Topic partitions to subscribe
        /// </summary>
        public IImmutableSet<TopicPartition> TopicPartitions { get; }

        /// <inheritdoc />
        public Option<IStatisticsHandler> StatisticsHandler { get; private set; }

        /// <inheritdoc />
        public ISubscription WithStatisticsHandler(IStatisticsHandler statisticsHandler)
        {
            StatisticsHandler = Option<IStatisticsHandler>.Create(statisticsHandler);
            return this;
        }
    }

    /// <summary>
    /// Assignment with offset subscription
    /// </summary>
    /// <remarks>
    /// Allows to subscribe to fixed set of topic partitions with initial offsets specified
    /// </remarks>
    internal sealed class AssignmentWithOffset : IManualSubscription
    {
        /// <summary>
        /// AssignmentWithOffset
        /// </summary>
        /// <param name="topicPartitions">List of topic partitions with offsets to subscribe</param>
        public AssignmentWithOffset(IImmutableSet<TopicPartitionOffset> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        /// <summary>
        /// List of topic partitions with offsets to subscribe
        /// </summary>
        public IImmutableSet<TopicPartitionOffset> TopicPartitions { get; }

        /// <inheritdoc />
        public Option<IStatisticsHandler> StatisticsHandler { get; private set; }

        /// <inheritdoc />
        public ISubscription WithStatisticsHandler(IStatisticsHandler statisticsHandler)
        {
            StatisticsHandler = Option<IStatisticsHandler>.Create(statisticsHandler);
            return this;
        }
    }

    /// <summary>
    /// Subscriptions
    /// </summary>
    public static class Subscriptions
    {
        /// <summary>
        /// Generates <see cref="TopicSubscription"/>
        /// </summary>
        /// <param name="topics">Topics to subscribe</param>
        public static IAutoSubscription Topics(params string[] topics) =>
            new TopicSubscription(topics.ToImmutableHashSet());
        
        /// <summary>
        /// Generates <see cref="TopicSubscriptionPattern"/>
        /// </summary>
        /// <param name="topicPattern">Topic pattern</param>
        public static IAutoSubscription TopicPattern(string topicPattern) =>
            new TopicSubscriptionPattern(topicPattern);

        /// <summary>
        /// Generates <see cref="Assignment"/>
        /// </summary>
        /// <param name="topicPartitions">Topic partitions to subscribe</param>
        public static IManualSubscription Assignment(params TopicPartition[] topicPartitions) =>
            new Assignment(topicPartitions.ToImmutableHashSet());

        /// <summary>
        /// Generates <see cref="AssignmentWithOffset"/>
        /// </summary>
        /// <param name="topicPartitions">Topic partitions with offsets to subscribe</param>
        public static IManualSubscription AssignmentWithOffset(params TopicPartitionOffset[] topicPartitions) =>
            new AssignmentWithOffset(topicPartitions.ToImmutableHashSet());
    }
}