using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
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

        private ConsumerSettings<Null, TOut> CreateConsumerSettings<TOut>(string group)
        {
            return ConsumerSettings<Null, TOut>.Create(Sys, null, null)
                .WithBootstrapServers(_fixture.KafkaServer)
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }
        
        private async Task ProduceStrings(TopicPartition topicPartition, IEnumerable<int> range, ProducerSettings<Null, string> producerSettings)
        {
            await Source
                .From(range)
                .Select(elem => new MessageAndMeta<Null, string> { TopicPartition = topicPartition, Message = new Message<Null, string> { Value = elem.ToString() } })
                .RunWith(KafkaProducer.PlainSink(producerSettings), _materializer);
        }

        private Tuple<Task, TestSubscriber.Probe<TOut>> CreateProbe<TOut>(IActorRef consumer, IManualSubscription sub)
        {
            return KafkaConsumer
                .PlainExternalSource<Null, TOut>(consumer, sub)
                .Where(c => !c.Value.Equals(InitialMsg))
                .Select(c => c.Value)
                .ToMaterialized(this.SinkProbe<TOut>(), Keep.Both)
                .Run(_materializer);
        }

        [Fact]
        public async Task ExternalPlainSource_with_external_consumer_Should_work()
        {
            var elementsCount = 10;
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            
            //Consumer is represented by actor
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(CreateConsumerSettings<string>(group)));
            
            //Manually assign topic partition to it
            var (partitionTask1, probe1) = CreateProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (partitionTask2, probe2) = CreateProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            
            // Produce messages to partitions
            await ProduceStrings(new TopicPartition(topic, new Partition(0)), Enumerable.Range(1, elementsCount), ProducerSettings);
            await ProduceStrings(new TopicPartition(topic, new Partition(1)), Enumerable.Range(1, elementsCount), ProducerSettings);

            // Request for produced messages and consume them
            probe1.Request(elementsCount);
            probe2.Request(elementsCount);
            probe1.Within(TimeSpan.FromSeconds(10), () => probe1.ExpectNextN(elementsCount));
            probe2.Within(TimeSpan.FromSeconds(10), () => probe2.ExpectNextN(elementsCount));
           
            // Stop stages
            probe1.Cancel();
            probe2.Cancel();
            
            // Make sure stages are stopped gracefully
            AwaitCondition(() => partitionTask1.IsCompletedSuccessfully && partitionTask2.IsCompletedSuccessfully);
            
            // Cleanup
            consumer.Tell(new KafkaConsumerActorMetadata.Internal.Stop(), ActorRefs.NoSender);
        }

        [Fact]
        public async Task ExternalPlainSource_should_be_stopped_on_serialization_error_only_when_requested_messages()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);

            // Make consumer expect numeric messages
            var settings = CreateConsumerSettings<int>(group).WithValueDeserializer(Deserializers.Int32);
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(settings));
            
            // Subscribe to partitions
            var (partitionTask1, probe1) = CreateProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (partitionTask2, probe2) = CreateProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            var (partitionTask3, probe3) = CreateProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 2)));

            // request from 2 streams
            probe1.Request(1);
            probe2.Request(1);
            await Task.Delay(500); // To establish demand

            // Send string messages
            await ProduceStrings(new TopicPartition(topic, 0), new int[] { 1 }, ProducerSettings);
            await ProduceStrings(new TopicPartition(topic, 1), new int[] { 1 }, ProducerSettings);

            // First two stages should fail, and only stage without demand should keep going
            probe1.ExpectError().Should().BeOfType<SerializationException>();
            probe2.ExpectError().Should().BeOfType<SerializationException>();
            probe3.Cancel();
            
            // Make sure source tasks finish accordingly
            AwaitCondition(() => partitionTask1.IsFaulted && partitionTask2.IsFaulted && partitionTask3.IsCompletedSuccessfully);
            
            // Cleanup
            consumer.Tell(new KafkaConsumerActorMetadata.Internal.Stop(), ActorRefs.NoSender);
        }
    }
}