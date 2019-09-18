using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class ExternalPlainSourceIntegrationTests : KafkaIntegrationTests
    {
        private const string InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it";
        
        private readonly KafkaFixture _fixture;
        private readonly ActorMaterializer _materializer;

        public ExternalPlainSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(ExternalPlainSourceIntegrationTests), output)
        {
            _fixture = fixture;
            _materializer = Sys.Materializer();
        }

        private string Uuid { get; } = Guid.NewGuid().ToString();

        private string CreateTopic(int number) => $"topic-{number}-{Uuid}";
        private string CreateGroup(int number) => $"group-{number}-{Uuid}";

        private ProducerSettings<Null, string> ProducerSettings
        {
            get => ProducerSettings<Null, string>.Create(Sys, null, null).WithBootstrapServers(_fixture.KafkaServer);
        }

        private ConsumerSettings<Null, string> CreateConsumerSettings(string group)
        {
            return ConsumerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers(_fixture.KafkaServer)
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }
        
        private async Task Produce(TopicPartition topicPartition, IEnumerable<int> range, ProducerSettings<Null, string> producerSettings)
        {
            await Source
                .From(range)
                .Select(elem => new MessageAndMeta<Null, string> { TopicPartition = topicPartition, Message = new Message<Null, string> { Value = elem.ToString() } })
                .RunWith(KafkaProducer.PlainSink(producerSettings), _materializer);
        }

        private Tuple<Task, TestSubscriber.Probe<string>> CreateProbe(IActorRef consumer, IManualSubscription sub)
        {
            return KafkaConsumer
                .PlainExternalSource<Null, string>(consumer, sub)
                .Where(c => !c.Value.Equals(InitialMsg))
                .Select(c => c.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(_materializer);
        }

        [Fact]
        public async Task ExternalPlainSource_with_external_consumer_Should_work()
        {
            var elementsCount = 10;
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            
            // await GivenInitializedTopic(topic, 2);

            //Consumer is represented by actor
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(CreateConsumerSettings(group)));
            
            //Manually assign topic partition to it
            var (partitionTask1, probe1) = CreateProbe(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (partitionTask2, probe2) = CreateProbe(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            
            await Produce(new TopicPartition(topic, new Partition(0)), Enumerable.Range(1, elementsCount), ProducerSettings);
            await Produce(new TopicPartition(topic, new Partition(1)), Enumerable.Range(1, elementsCount), ProducerSettings);

            probe1.Request(elementsCount);
            probe2.Request(elementsCount);

            probe1.Within(TimeSpan.FromSeconds(10), () => probe1.ExpectNextN(elementsCount));
            probe2.Within(TimeSpan.FromSeconds(10), () => probe2.ExpectNextN(elementsCount));
           
            probe1.Cancel();
            probe2.Cancel();
            
            AwaitCondition(() => partitionTask1.IsCompletedSuccessfully && partitionTask2.IsCompletedSuccessfully);
            
            consumer.Tell(new KafkaConsumerActorMetadata.Internal.Stop(), ActorRefs.NoSender);
        }
    }
}