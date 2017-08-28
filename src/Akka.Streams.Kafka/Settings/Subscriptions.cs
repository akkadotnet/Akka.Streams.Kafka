using System.Collections.Immutable;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Settings
{
    public interface ISubscription { }
    public interface IManualSubscription : ISubscription { }
    public interface IAutoSubscription : ISubscription { }

    internal sealed class TopicSubscription : IAutoSubscription
    {
        public TopicSubscription(IImmutableSet<string> topics)
        {
            Topics = topics;
        }

        public IImmutableSet<string> Topics { get; }
    }

    internal sealed class TopicSubscriptionPattern : IAutoSubscription
    {
        public TopicSubscriptionPattern(string pattern)
        {
            Pattern = pattern;
        }

        public string Pattern { get; }
    }

    internal sealed class Assignment : IManualSubscription
    {
        public Assignment(IImmutableSet<TopicPartition> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        public IImmutableSet<TopicPartition> TopicPartitions { get; }
    }

    public static class Subscriptions
    {
        public static IAutoSubscription Topics(params string[] topics) => new TopicSubscription(topics.ToImmutableHashSet());

        public static IAutoSubscription TopicPattern(string pattern) => new TopicSubscriptionPattern(pattern);

        public static IManualSubscription Assignment(params TopicPartition[] topicPartitions) =>
            new Assignment(topicPartitions.ToImmutableHashSet());
    }
}