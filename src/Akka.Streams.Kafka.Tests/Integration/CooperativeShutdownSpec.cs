// -----------------------------------------------------------------------
//  <copyright file="CooperativeShutdownSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2026 - 2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Xunit;
using LogMessage = Confluent.Kafka.LogMessage;

namespace Akka.Streams.Kafka.Tests.Integration;

public class CooperativeShutdownSpec : KafkaIntegrationTests
{
    public CooperativeShutdownSpec(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(CooperativeShutdownSpec), output, fixture)
    {
    }

    [Theory]
    [InlineData("range")]
    [InlineData("cooperative-sticky")]
    public async Task Shutdown_should_unassign_partitions_with_the_api_of_the_rebalance_protocol(string strategy)
    {
        var topic = CreateTopic(1);
        var group = CreateGroup(1);
        await ProduceStrings(new TopicPartition(topic, 0), Enumerable.Range(1, 1), ProducerSettings);

        var librdkafkaLog = new ConcurrentQueue<LogMessage>();
        var settings = CreateConsumerSettings<string>(group)
            .WithProperty("partition.assignment.strategy", strategy)
            .WithConsumerFactory(s => BuildConsumerCapturingLog(s, librdkafkaLog));
        var (control, probe) = CreateProbe(settings, topic);
        probe.Request(1);
        probe.ExpectNext(TimeSpan.FromSeconds(30));

        await control.Shutdown();

        // Under cooperative-sticky a plain Unassign() fails with "application *assign() call failed:
        // Changes to the current assignment must be made using incremental_assign() or incremental_unassign()"
        librdkafkaLog.Where(m => m.Facility == "ASSIGN").Select(m => m.Message).Should().BeEmpty();

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Fixture.KafkaServer }).Build();
        await AwaitAssertAsync(async () =>
        {
            var description = await admin.DescribeConsumerGroupsAsync(new[] { group });
            description.ConsumerGroupDescriptions.Single().State.Should().Be(ConsumerGroupState.Empty);
        }, TimeSpan.FromSeconds(10));
    }

    private static IConsumer<Null, string> BuildConsumerCapturingLog(ConsumerSettings<Null, string> settings,
        ConcurrentQueue<LogMessage> log)
    {
        var listener = settings.RebalanceListener;
        return new ConsumerBuilder<Null, string>(settings.Properties)
            .SetLogHandler((_, message) => log.Enqueue(message))
            .SetPartitionsAssignedHandler((c, partitions) =>
                listener?.OnPartitionAssigned?.Invoke(c, partitions) ??
                partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)))
            .SetPartitionsRevokedHandler((c, partitions) => listener?.OnPartitionRevoked?.Invoke(c, partitions))
            .SetPartitionsLostHandler((c, partitions) => listener?.OnPartitionLost?.Invoke(c, partitions))
            .Build();
    }
}
