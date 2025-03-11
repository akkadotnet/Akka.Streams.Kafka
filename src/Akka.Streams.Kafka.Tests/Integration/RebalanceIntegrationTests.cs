using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Util;
using Confluent.Kafka;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class RebalanceIntegrationTests : KafkaIntegrationTests
{
    public RebalanceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(AtMostOnceSourceIntegrationTests), output, fixture)
    {
    }

    private static readonly IReadOnlyList<int> Numbers = Enumerable.Range(0, 5000).ToList();
    private const string ConsumerClientId1 = "consumer-1";
    private const string ConsumerClientId2 = "consumer-2";

    private static Source<ConsumeResult<Null, string>, IControl> GetConsumer(
        ConsumerSettings<Null, string> settings,
        string topic)
    {
        return KafkaConsumer.PlainSource(settings, Subscriptions.Topics(topic));
    }

    private sealed class StreamCompleted
    {
        public static StreamCompleted Instance { get; } = new();

        private StreamCompleted()
        {
        }
    }

    private sealed record StreamFailed(Exception Ex);

    private IKillSwitch CreateKillableStream(string topic, ConsumerSettings<Null, string> settings, IActorRef sinkRef,
        string consumerName)
    {
        var source = GetConsumer(settings, topic);
        var killSwitch = source
            .WithAttributes(Attributes.CreateName(consumerName))
            .ViaMaterialized(KillSwitches.Single<ConsumeResult<Null, string>>(), Keep.Right)
            .WithAttributes(Attributes.CreateName($"selectAsync-{consumerName}"))
            .ToMaterialized(
                Sink.ActorRef<ConsumeResult<Null, string>>(sinkRef, StreamCompleted.Instance,
                    exception => new StreamFailed(exception)), Keep.Left)
            .Run(Materializer.WithNamePrefix(consumerName));

        return killSwitch;
    }

    /// <summary>
    /// Reproduction spec for https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415
    /// </summary>
    [Fact]
    public async Task FetchedRecords_must_be_removed_from_source_stage_buffer_when_partition_is_removed()
    {
        // arrange
        const int count = 20;
        var topic = CreateTopic(1);
        var group = CreateGroup(1);
        var tp0 = new TopicPartition(topic, 0);
        var tp1 = new TopicPartition(topic, 1);

        // initialize the topic with 10 partitions
        await GivenInitializedTopicAsync(tp1, 2);

        var settings = CreateConsumerSettings<Null, string>(group);

        // Produce some messages
        await ProduceStrings(tp1, Enumerable.Range(0, count), ProducerSettings);

        Log.Debug("Subscribe to the topic (without downstream demand)");
        var probe1RebalanceActor = CreateTestProbe();
        var probe1Subscription = Subscriptions.Topics(topic).WithRebalanceListener(probe1RebalanceActor.Ref);

        var (control1, probe1) = KafkaConsumer.PlainSource(settings.WithClientId(ConsumerClientId1), probe1Subscription)
            .ToMaterialized(this.SinkProbe<ConsumeResult<Null, string>>(), Keep.Both)
            .Run(Materializer);

        Log.Debug("Await initial partition assignment");
        (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions.Should()
            .BeEquivalentTo([tp0, tp1]);

        Log.Debug("Read one message from probe1 with partition 1");
        var m = await probe1.RequestNextAsync();

        Log.Debug("Subscribe to the topic (without downstream demand)");
        var probe2RebalanceActor = CreateTestProbe();
        var probe2Subscription = Subscriptions.Topics(topic).WithRebalanceListener(probe2RebalanceActor.Ref);
        var (control2, probe2) = KafkaConsumer.PlainSource(settings.WithClientId(ConsumerClientId2), probe2Subscription)
            .ToMaterialized(this.SinkProbe<ConsumeResult<Null, string>>(), Keep.Both)
            .Run(Materializer);

        // All partitions should be revoked from consumer 1
        Log.Debug("Await a revoke to consumer 1");
        var revokedPartitions = (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsRevoked>()).Partitions;
        Log.Debug("Revoked partitions [{0}] from consumer 1", revokedPartitions);

        // Single partition should be assigned back to both consumers
        Log.Debug("Await assign to consumer 2");
        var assignedPartitionConsumer2 =
            (await probe2RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions.Single();
        Log.Debug("Assigned partition [{0}] to consumer 2", assignedPartitionConsumer2);

        Log.Debug("Awaiting assign to consumer 1");
        var assignedPartitionConsumer1 =
            (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions.Single();
        Log.Debug("Assigned partition [{0}] to consumer 1", assignedPartitionConsumer1);

        // sanity check
        Assert.NotEqual(assignedPartitionConsumer1, assignedPartitionConsumer2);

        Log.Debug("Resume demand on both consumers");
        _ = probe1.RequestAsync(count);
        _ = probe2.RequestAsync(count);

        // winning node is whoever was assigned to partition1
        var (winningConsumer, losingConsumer) 
            = assignedPartitionConsumer1 == tp1 ? (probe1, probe2) : (probe2, probe1);
        
        Log.Debug("Read all messages from winning consumer");
        var winningMessages = await winningConsumer.ExpectNextNAsync(count).ToListAsync();
        
        Log.Debug("Expect no messages on losing consumer");
        await losingConsumer.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(500));
        
        Assert.Equal(count, winningMessages.Count);

        await probe1.CancelAsync();
        await probe2.CancelAsync();
        
        Assert.True(control1.IsShutdown.IsCompleted);
        Assert.True(control2.IsShutdown.IsCompleted);
    }
}