// -----------------------------------------------------------------------
//  <copyright file="ConsumerResultFactory.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Tests.TestKit.Internal;

internal static class ConsumerResultFactory
{
    public static MockCommitter FakeCommiter { get; } = new();

    public static GroupTopicPartitionOffset PartitionOffset(string groupId, string topic, int partition, long offset) =>
        new(groupId, topic, partition, offset);

    public static GroupTopicPartitionOffset PartitionOffset(GroupTopicPartition key, long offset) => new(key, offset);

    internal static CommittableOffset CommittableOffset(string groupId, string topic, int partition, long offset,
        string metadata)
        => CommittableOffset(PartitionOffset(groupId, topic, partition, offset), metadata);

    internal static CommittableOffset CommittableOffset(GroupTopicPartitionOffset partitionOffset, string metadata) =>
        new(FakeCommiter, partitionOffset, metadata);

    internal class MockCommitter : KafkaAsyncConsumerCommitter
    {
        public MockCommitter() : base(() => ActorRefs.Nobody, TimeSpan.Zero)
        {
        }

        public override Task CommitSingle(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) =>
            Task.CompletedTask;

        public override Task CommitOneOfMany(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata)
            => Task.CompletedTask;

        public override void TellCommit(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata,
            bool emergency)
        {
        }
    }
}