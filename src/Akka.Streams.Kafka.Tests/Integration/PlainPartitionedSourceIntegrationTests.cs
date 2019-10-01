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
            int partitionsCount = 3;
            int totalMessages = 1000 * 10;

            var consumerConfig = CreateConsumerSettings<string>(group)
                .WithProperty("metadata.max.age.ms", "100");  // default was 5 * 60 * 1000 (five minutes)

            var (consumeTask, probe) = KafkaConsumer.PlainPartitionedSource(consumerConfig, Subscriptions.Topics(topic))
                .GroupBy(4, tuple => tuple.Item1)
                .SelectAsync(8, async tuple =>
                {
                    var (topicPartition, source) = tuple;
                    Log.Info($"Sub-source for {topicPartition}");
                    var sourceMessages = await source
                        .Scan(0, (i, message) => i++)
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

            var produceTask = Source.From(Enumerable.Range(0, totalMessages))
                .Select(LogSentMessages)
                .Select(number =>
                {
                    if (number == totalMessages / 2)
                    {
                        Log.Warning($"Stopping one Kafka container after {number} messages");
                        // TODO: Stop one of multiple docker containers in kafka cluster
                    }

                    return number;
                })
                .Select(number => new MessageAndMeta<Null, string>()
                {
                    TopicPartition = new TopicPartition(topic, number % partitionsCount), 
                    Message = new Message<Null, string>() { Value = number.ToString() }
                })
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);
            
            probe.Request(totalMessages);
            
            AwaitCondition(() => produceTask.IsCompletedSuccessfully, TimeSpan.FromSeconds(30));
            
            probe.Cancel();
            foreach (var i in Enumerable.Range(0, totalMessages))
            {
                probe.ExpectNext(i);
            }
            
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