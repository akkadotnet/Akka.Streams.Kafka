// -----------------------------------------------------------------------
//  <copyright file="RebalanceIntegrationTests.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

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
using Akka.Util.Internal;
using Confluent.Kafka;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Integration;

public class RebalanceIntegrationTests : KafkaIntegrationTests
{
    public RebalanceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(RebalanceIntegrationTests), output, fixture)
    {
    }

    private const string ConsumerClientId1 = "consumer-1";
    private const string ConsumerClientId2 = "consumer-2";

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
        var partitions1 = (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions;
        Assert.True(partitions1.Contains(tp0) && partitions1.Contains(tp1) && partitions1.Count == 2);

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

        await control1.IsShutdown.WaitAsync(RemainingOrDefault);
        await control2.IsShutdown.WaitAsync(RemainingOrDefault);
    }

    [Fact]
    public async Task FetchedRecords_must_be_removed_from_the_partitioned_source_stage_when_a_partition_is_revoked()
    {
        const int count = 20;
        var topic = CreateTopic(1);
        var group = CreateGroup(1);
        var tp0 = new TopicPartition(topic, 0);
        var tp1 = new TopicPartition(topic, 1);

        await GivenInitializedTopicAsync(tp1, 2);

        var settings = CreateConsumerSettings<Null, string>(group);

        await ProduceStrings(tp1, Enumerable.Range(0, count), ProducerSettings);

        Log.Debug("Subscribe to the topic (without downstream demand)");
        var probe1RebalanceActor = CreateTestProbe();
        var probe1Subscription = Subscriptions.Topics(topic).WithRebalanceListener(probe1RebalanceActor.Ref);
        var (control1, probe1) = KafkaConsumer
            .PlainPartitionedSource(settings.WithClientId(ConsumerClientId1), probe1Subscription)
            .ToMaterialized(this.SinkProbe<(TopicPartition, Source<ConsumeResult<Null, string>, NotUsed>)>(), Keep.Both)
            .Run(Materializer);

        Log.Debug("Await initial partition assignment");
        var partitions2 = (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions;
        Assert.True(partitions2.Contains(tp0) && partitions2.Contains(tp1) && partitions2.Count == 2);

        Log.Debug("Read 2 sub-sources returned by the partitioned source");
        await probe1.RequestAsync(2);

        var probe1RunningSubSourceProbes = await SubSourcesWithProbes(2, probe1);

        Log.Debug("Read one message from probe1 with partition 1");
        probe1RunningSubSourceProbes.Where(c => c.Item1.Partition == 1)
            .ForEach(c => c.Item2.RequestNext());

        Log.Debug("Subscribe to the topic (without downstream demand)");
        var probe2RebalanceActor = CreateTestProbe();
        var probe2Subscription = Subscriptions.Topics(topic).WithRebalanceListener(probe2RebalanceActor.Ref);
        var (control2, probe2) = KafkaConsumer
            .PlainPartitionedSource(settings.WithClientId(ConsumerClientId2), probe2Subscription)
            .ToMaterialized(this.SinkProbe<(TopicPartition, Source<ConsumeResult<Null, string>, NotUsed>)>(), Keep.Both)
            .Run(Materializer);

        await probe2.RequestAsync(1);
        var probe2RunningSubSourceProbes = await SubSourcesWithProbes(1, probe2);

        // All partitions should be revoked from consumer 1
        Log.Debug("Await a revoke to consumer 1");
        var revokedPartitions = (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsRevoked>()).Partitions;
        Log.Debug("Revoked partitions [{0}] from consumer 1", revokedPartitions);

        // Single partition should be assigned back to both consumers
        Log.Debug("Await assign to consumer 2");
        var assignedPartitionConsumer2 =
            (await probe2RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions.Single();

        Log.Debug("Awaiting assign to consumer 1");
        var assignedPartitionConsumer1 =
            (await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>()).Partitions.Single();

        // sanity check
        Assert.NotEqual(assignedPartitionConsumer1, assignedPartitionConsumer2);

        // get the winning and losing consumers
        var (winningConsumer, losingConsumer)
            = assignedPartitionConsumer1 == tp1
                ? (probe1RunningSubSourceProbes, probe2RunningSubSourceProbes)
                : (probe2RunningSubSourceProbes, probe1RunningSubSourceProbes);

        Log.Debug("Resume demand on both consumers");
        RunForSubSource(1, probe1RunningSubSourceProbes, c => c.Request(count));
        RunForSubSource(1, probe2RunningSubSourceProbes, c => c.Request(count));

        Log.Debug("No further messages should be emitted from the losing consumer");
        RunForSubSource(1, losingConsumer, c => c.ExpectComplete());

        var winningMessages = winningConsumer
            .Where(c => c.Item1.Partition.Value == 1)
            .SelectMany(c => c.Item2.ExpectNextN(count))
            .ToList();

        Assert.Equal(count, winningMessages.Count);

        await probe1.CancelAsync();
        await probe2.CancelAsync();

        await control1.IsShutdown.WaitAsync(RemainingOrDefault);
        await control2.IsShutdown.WaitAsync(RemainingOrDefault);

        return;

        ValueTask<List<(TopicPartition, TestSubscriber.Probe<ConsumeResult<Null, string>>)>> SubSourcesWithProbes(
            int partitions,
            TestSubscriber.Probe<(TopicPartition, Source<ConsumeResult<Null, string>, NotUsed>)> probe)
        {
            return probe.ExpectNextNAsync(partitions)
                .Select(c =>
                {
                    var (tp, source) = c;
                    var subProbe = source.RunWith(this.SinkProbe<ConsumeResult<Null, string>>(), Materializer);
                    return (tp, subProbe);
                }).ToListAsync();
        }

        void RunForSubSource(int partition,
            List<(TopicPartition, TestSubscriber.Probe<ConsumeResult<Null, string>>)> subSourcesWithProbes,
            Action<TestSubscriber.Probe<ConsumeResult<Null, string>>> fun)
        {
            foreach (var (_, probe) in subSourcesWithProbes
                         .Where(c => c.Item1.Partition == partition))
            {
                fun(probe);
            }
        }
    }
}