using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
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
    public class PlainSinkIntegrationTests : KafkaIntegrationTests
    {
        public PlainSinkIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(null, output, fixture)
        {
        }

        [Fact]
        public async Task PlainSink_should_publish_100_elements_to_Kafka_producer()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            var consumerSettings = CreateConsumerSettings<string>(group1);
            var consumer = consumerSettings.CreateKafkaConsumer();
            consumer.Assign(new List<TopicPartition> { topicPartition1 });

            var task = new TaskCompletionSource<NotUsed>();
            int messagesReceived = 0;

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(c => c.ToString())
                .Select(elem => new MessageAndMeta<Null, string> { TopicPartition = topicPartition1, Message = new Message<Null, string> { Value = elem } })
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);

            var dateTimeStart = DateTime.UtcNow;

            bool CheckTimeout(TimeSpan timeout)
            {
                return dateTimeStart.AddSeconds(timeout.TotalSeconds) > DateTime.UtcNow;
            }

            while (!task.Task.IsCompleted && CheckTimeout(TimeSpan.FromMinutes(1)))
            {
                try
                {
                    consumer.Consume(TimeSpan.FromSeconds(1));
                    messagesReceived++;
                    if (messagesReceived == 100)
                        task.SetResult(NotUsed.Instance);
                }
                catch (ConsumeException) { /* Just try again */ }
            }

            messagesReceived.Should().Be(100);
        }

        [Fact]
        public async Task PlainSink_should_fail_stage_if_broker_unavailable()
        {
            var topic1 = CreateTopic(1);

            await GivenInitializedTopic(topic1);

            var config = ProducerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers("localhost:10092");

            var probe = Source
                .From(Enumerable.Range(1, 100))
                .Select(c => c.ToString())
                .Select(elem => new MessageAndMeta<Null, string> { Topic = topic1, Message = new Message<Null, string> { Value = elem } })
                .Via(KafkaProducer.PlainFlow(config))
                .RunWith(this.SinkProbe<DeliveryReport<Null, string>>(), Materializer);

            probe.ExpectSubscription();
            probe.OnError(new KafkaException(ErrorCode.Local_Transport));
        }
    }
}
