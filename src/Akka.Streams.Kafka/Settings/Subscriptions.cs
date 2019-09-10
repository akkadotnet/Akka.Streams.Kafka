using System.Collections.Immutable;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Settings
{
    public interface ISubscription { }
    public interface IManualSubscription : ISubscription { }
    public interface IAutoSubscription : ISubscription { }

    /// <summary>
    /// TopicSubscription
    /// </summary>
    internal sealed class TopicSubscription : IAutoSubscription
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
        public IImmutableSet<string> Topics { get; }
    }
    
    /// <summary>
    /// TopicSubscriptionPattern
    /// </summary>
    internal sealed class TopicSubscriptionPattern : IAutoSubscription
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
        public string TopicPattern { get; }
    }

    internal sealed class Assignment : IManualSubscription
    {
        public Assignment(IImmutableSet<TopicPartition> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        public IImmutableSet<TopicPartition> TopicPartitions { get; }
    }

    internal sealed class AssignmentWithOffset : IManualSubscription
    {
        public AssignmentWithOffset(IImmutableSet<TopicPartitionOffset> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        public IImmutableSet<TopicPartitionOffset> TopicPartitions { get; }
    }

    public static class Subscriptions
    {
        public static IAutoSubscription Topics(params string[] topics) =>
            new TopicSubscription(topics.ToImmutableHashSet());

        public static IManualSubscription Assignment(params TopicPartition[] topicPartitions) =>
            new Assignment(topicPartitions.ToImmutableHashSet());

        public static IManualSubscription AssignmentWithOffset(params TopicPartitionOffset[] topicPartitions) =>
            new AssignmentWithOffset(topicPartitions.ToImmutableHashSet());
    }
}