using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class CommittableSourceIntegrationTests : Akka.TestKit.Xunit2.TestKit
    {
        private const string KafkaUrl = "localhost:9092";

        private const string InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it";

        private readonly ActorMaterializer _materializer;

        public CommittableSourceIntegrationTests(ITestOutputHelper output) 
            : base(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"), null, output)
        {
            _materializer = Sys.Materializer();
        }

        private string Uuid { get; } = Guid.NewGuid().ToString();

        private string CreateTopic(int number) => $"topic-{number}-{Uuid}";
        private string CreateGroup(int number) => $"group-{number}-{Uuid}";

        private ProducerSettings<Null, string> ProducerSettings =>
            ProducerSettings<Null, string>.Create(Sys, null, new StringSerializer(Encoding.UTF8))
                .WithBootstrapServers(KafkaUrl);

        private async Task GivenInitializedTopic(string topic)
        {
            var producer = ProducerSettings.CreateKafkaProducer();
            await producer.ProduceAsync(topic, null, InitialMsg, 0);
            producer.Dispose();
        }

        private ConsumerSettings<Null, string> CreateConsumerSettings(string group)
        {
            return ConsumerSettings<Null, string>.Create(Sys, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers(KafkaUrl)
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }

        [Fact]
        public async Task PlainSource_consumes_messages_from_KafkaProducer_with_topicPartition_assignment()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await GivenInitializedTopic(topic1);

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(elem => new ProduceRecord<Null, string>(topic1, null, elem.ToString()))
                .Via(Dsl.Producer.CreateFlow(ProducerSettings))
                .RunWith(Sink.Ignore<Message<Null, string>>(), _materializer);

            var consumerSettings = CreateConsumerSettings(group1);

            var committedElements = new ConcurrentQueue<string>();

            var (_, probe1) = Dsl.Consumer.CommitableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                //.WhereNot(c => c.Record.Value == InitialMsg)
                .SelectAsync(10, elem =>
                {
                    return elem.CommitableOffset.Commit().ContinueWith(t =>
                    {
                        committedElements.Enqueue(elem.Record.Value);
                        return Done.Instance;
                    });
                })
                .ToMaterialized(this.SinkProbe<Done>(), Keep.Both)
                .Run(_materializer);

            probe1
                .Request(25)
                .ExpectNextN(25)
                .All(c => c is Done)
                .Should()
                .BeTrue();

            probe1.Cancel();
        }
    }
}
