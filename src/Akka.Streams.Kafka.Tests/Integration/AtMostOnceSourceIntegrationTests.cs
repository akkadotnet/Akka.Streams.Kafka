using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class AtMostOnceSourceIntegrationTests : KafkaIntegrationTests
    {
        public AtMostOnceSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(AtMostOnceSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task AtMostOnceSource_Should_stop_consuming_actor_when_used_with_Take()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);

            await ProduceStrings(new TopicPartition(topic, 0), Enumerable.Range(1, 10), ProducerSettings);
            
            var (control, result) = KafkaConsumer.AtMostOnceSource(CreateConsumerSettings<string>(group), Subscriptions.Assignment(new TopicPartition(topic, 0)))
                .Select(m => m.Value)
                .Take(5)
                .ToMaterialized(Sink.Seq<string>(), Keep.Both)
                .Run(Materializer);
            
            AwaitCondition(() => control.IsShutdown.IsCompletedSuccessfully, TimeSpan.FromSeconds(10));
            
            result.Result.Should().BeEquivalentTo(Enumerable.Range(1, 5).Select(i => i.ToString()));
        }

        [Fact(Skip = "Issue https://github.com/akkadotnet/Akka.Streams.Kafka/issues/66")]
        public async Task AtMostOnceSource_Should_work()
        {
            var topic = CreateTopic(1);
            var settings = CreateConsumerSettings<string>(CreateGroup(1));
            var totalMessages = 10;
            var lastMessage = new TaskCompletionSource<Done>();
            
            await ProduceStrings(topic, Enumerable.Range(1, 10), ProducerSettings);

            var (task, probe) = KafkaConsumer.AtMostOnceSource(settings, Subscriptions.Topics(topic))
                .SelectAsync(1, m =>
                {
                    if (m.Value == totalMessages.ToString())
                        lastMessage.SetResult(Done.Instance);

                    return Task.FromResult(Done.Instance);
                })
                .ToMaterialized(this.SinkProbe<Done>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);

            await lastMessage.Task;
           
            probe.Cancel();
            
            probe.ExpectNextN(10);
        }
    }
}