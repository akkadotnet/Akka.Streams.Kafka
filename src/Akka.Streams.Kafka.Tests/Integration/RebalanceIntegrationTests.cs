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

    private IKillSwitch CreateKillableStream(string topic, ConsumerSettings<Null, string> settings, IActorRef sinkRef, string consumerName)
    {
        var source = GetConsumer(settings, topic);
        var killSwitch = source
            .WithAttributes(Attributes.CreateName(consumerName))
            .ViaMaterialized(KillSwitches.Single<CommittableMessage<Null, string>>(), Keep.Right)
            .SelectAsync(1, async msg =>
            {
                // commit the offset
                await msg.CommitableOffset.Commit();
                return msg.Record;
            })
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

        var killSwitch1 = CreateKillableStream(topic, settings, probe1.Ref, "consumer1");
        var killSwitch2 = CreateKillableStream(topic, settings, probe1.Ref, "consumer2");
        var killSwitch3 = CreateKillableStream(topic, settings, probe1.Ref, "consumer3");

        // make sure all 10 messages got processed
        var msgs1 = await probe1.ReceiveNAsync(10).Cast<ConsumeResult<Null, string>>().ToListAsync();
        
        // act

        // per https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415 - it might take many restart attempts to reproduce
        const int restartAttempts = 100;
        for (var i = 0; i < restartAttempts; i++)
        {
            killSwitch1 = await KillAndRelaunchFirstConsumer(killSwitch1, i);
        }

        return;

        async Task<IKillSwitch> KillAndRelaunchFirstConsumer(IKillSwitch ks, int attemptCount)
        {
            Sys.Log.Info("Restarting consumer, attempt {0}", attemptCount);
            
            // kill the first consumer
            ks.Shutdown();
            
            // produce more messages
            await ProduceStrings(topic, Enumerable.Range(10, 30), ProducerSettings); // let it run as a detatched task
            
            // relaunch the first consumer
            var newKs = CreateKillableStream(topic, settings, probe1.Ref, $"consumer1-{attemptCount}");
        
            // produce more messages
            await ProduceStrings(topic, Enumerable.Range(10, 30), ProducerSettings); // let it run as a detatched task
        
            var msg2 = await probe1.FishForMessageAsync(c => c is StreamCompleted or StreamFailed);
            if (msg2 is StreamFailed failure)
            {
                throw new Exception($"Stream failed due to {failure.Ex.Message}", failure.Ex);
            }

            return newKs;
        }
    }
}