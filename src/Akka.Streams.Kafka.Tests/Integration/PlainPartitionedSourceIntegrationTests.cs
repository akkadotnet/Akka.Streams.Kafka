using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class PlainPartitionedSourceIntegrationTests : KafkaIntegrationTests
    {
        public PlainPartitionedSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(PlainPartitionedSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task PlainPartitionedSource_Should_not_lose_any_messages_when_Kafka_node_dies()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 1000 * 10;

            var consumerSettings = CreateConsumerSettings<string>(group);

            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);

            var (consumeTask, probe) = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .GroupBy(3, tuple => tuple.Item1)
                .SelectAsync(8, async tuple =>
                {
                    var (topicPartition, source) = tuple;
                    Log.Info($"Sub-source for {topicPartition}");
                    var sourceMessages = await source
                        .Scan(0, (i, message) => i + 1)
                        .Select(i => LogReceivedMessages(topicPartition, i))
                        .RunWith(Sink.Last<long>(), Materializer);

                    Log.Info($"{topicPartition}: Received {sourceMessages} messages in total");
                    return sourceMessages;
                })
                .MergeSubstreams()
                .As<Source<long, Task>>()
                .Scan(0L, (i, subValue) => i + subValue)
                .ToMaterialized(this.SinkProbe<long>(), Keep.Both)
                .Run(Materializer);
            
            AwaitCondition(() =>
            {
                Log.Debug("Expecting next number...");
                var next = probe.RequestNext(TimeSpan.FromSeconds(10));
                Log.Debug("Got requested number: " + next);
                return next == totalMessages;
            }, TimeSpan.FromSeconds(20));

            probe.Cancel();

            AwaitCondition(() => consumeTask.IsCompletedSuccessfully);
        }

        private int LogSentMessages(int counter)
        {
            if (counter % 1000 == 0)
                Log.Info($"Sent {counter} messages so far");
            
            return counter;
        }
        
        private long LogReceivedMessages(TopicPartition tp, int counter)
        {
            if (counter % 1000 == 0)
                Log.Info($"{tp}: Received {counter} messages so far.");

            return counter;
        }
    }
}