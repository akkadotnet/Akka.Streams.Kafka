using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class RebalanceIntegrationTests : KafkaIntegrationTests
{
    public RebalanceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(AtMostOnceSourceIntegrationTests), output, fixture)
    {
    }

    private static Source<CommittableMessage<Null, string>, IControl> GetConsumer(
        ConsumerSettings<Null, string> settings,
        string topic)
    {
        return KafkaConsumer.CommittableSource(settings, Subscriptions.Topics(topic));
    }

    private sealed class StreamCompleted
    {
        public static StreamCompleted Instance { get; } = new();

        private StreamCompleted()
        {
        }
    }

    private sealed record StreamFailed(Exception Ex);

    private IKillSwitch CreateKillableStream(string topic, ConsumerSettings<Null, string> settings, IActorRef sinkRef)
    {
        var source = GetConsumer(settings, topic);
        var killSwitch = source.ViaMaterialized(KillSwitches.Single<CommittableMessage<Null, string>>(), Keep.Right)
            .SelectAsync(1, async msg =>
            {
                // commit the offset
                await msg.CommitableOffset.Commit();
                return msg.Record;
            })
            .ToMaterialized(
                Sink.ActorRef<ConsumeResult<Null, string>>(sinkRef, StreamCompleted.Instance,
                    exception => new StreamFailed(exception)), Keep.Left)
            .Run(Materializer);

        return killSwitch;
    }

    [Fact]
    public async Task ShouldReBalanceWithoutArgumentExceptions()
    {
        // arrange
        var topic = CreateTopic(1);
        var group = CreateGroup(1);
        const int partitions = 10;
        const int totalMessages = 100;

        // initialize the topic with 10 partitions
        await GivenInitializedTopicAsync(topic, partitions);

        var settings = CreateConsumerSettings<Null, string>(group);

        // Produce some messages
        await ProduceStrings(topic, Enumerable.Range(0, 10), ProducerSettings);

        // Spin up 3 consumers
        var probe1 = CreateTestProbe();

        var killSwitch1 = CreateKillableStream(topic, settings, probe1.Ref);
        var killSwitch2 = CreateKillableStream(topic, settings, probe1.Ref);
        var killSwitch3 = CreateKillableStream(topic, settings, probe1.Ref);

        // make sure all 10 messages got processed
        var msgs = await probe1.ReceiveNAsync(10).Cast<ConsumeResult<Null, string>>().ToListAsync();
    }
}