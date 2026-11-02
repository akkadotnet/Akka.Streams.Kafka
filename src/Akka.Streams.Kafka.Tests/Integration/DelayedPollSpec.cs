// -----------------------------------------------------------------------
//  <copyright file="DelayedPollSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2026 - 2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Integration;

public class DelayedPollSpec : KafkaIntegrationTests
{
    private static readonly TimeSpan LongPollInterval = TimeSpan.FromSeconds(10);

    public DelayedPollSpec(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(DelayedPollSpec), output, fixture)
    {
    }

    [Fact]
    public async Task Commit_should_not_wait_for_the_next_periodic_poll()
    {
        var topic = CreateTopic(1);
        var group = CreateGroup(1);
        var partition = new TopicPartition(topic, 0);
        await ProduceStrings(partition, new[] { 1 }, ProducerSettings);

        // a periodic poll far slower than the commit is allowed to take
        var settings = CreateConsumerSettings<string>(group).WithPollInterval(LongPollInterval);
        var probe = KafkaConsumer.CommittableSource(settings, Subscriptions.Assignment(partition))
            .RunWith(this.SinkProbe<CommittableMessage<Null, string>>(), Materializer);

        probe.Request(1);
        var message = probe.ExpectNext(LongPollInterval * 3);

        // a commit asks the consumer actor for a delayed poll, which performs the commit right away
        var watch = Stopwatch.StartNew();
        await ((CommittableOffset)message.CommitableOffset).Commit();
        watch.Stop();

        watch.Elapsed.Should().BeLessThan(LongPollInterval / 2);

        probe.Cancel();
    }
}
