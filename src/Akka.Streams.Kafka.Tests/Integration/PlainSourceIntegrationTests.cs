using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Tests.Logging;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Hocon.Config;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class PlainSourceIntegrationTests : KafkaIntegrationTests
    {
        public PlainSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(PlainSourceIntegrationTests), output, fixture)
        {
        }
    

        private (IControl, TestSubscriber.Probe<string>) CreateProbe(ConsumerSettings<Null, string> consumerSettings, ISubscription sub)
        {
            return KafkaConsumer
                .PlainSource(consumerSettings, sub)
                .Where(c => !c.Value.Equals(InitialMsg))
                .Select(c => c.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);
        }

        [Fact]
        public async Task PlainSource_consumes_messages_from_KafkaProducer_with_topicPartition_assignment()
        {
            int elementsCount = 100;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            await ProduceStrings(topicPartition1, Enumerable.Range(1, elementsCount), ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group1);

            var (_, probe) = CreateProbe(consumerSettings, Subscriptions.Assignment(topicPartition1));
            
            probe.Request(elementsCount);
            foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
                probe.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe.Cancel();
        }

        [Fact]
        public async Task PlainSource_consumes_messages_from_KafkaProducer_with_topicPartitionOffset_assignment()
        {
            int elementsCount = 100;
            int offset = 50;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            await ProduceStrings(topicPartition1, Enumerable.Range(1, elementsCount), ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group1);

            var (_, probe) = CreateProbe(consumerSettings, Subscriptions.AssignmentWithOffset(new TopicPartitionOffset(topicPartition1, new Offset(offset))));

            probe.Request(elementsCount);
            foreach (var i in Enumerable.Range(offset, elementsCount - offset).Select(c => c.ToString()))
                probe.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe.Cancel();
        }

        [Fact]
        public async Task PlainSource_consumes_messages_from_KafkaProducer_with_subscribe_to_topic()
        {
            int elementsCount = 100;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group1);

            var (control, probe) = CreateProbe(consumerSettings, Subscriptions.Topics(topic1));

            probe.Request(elementsCount);
            foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
                probe.ExpectNext(i, TimeSpan.FromSeconds(10));

            var shutdown = control.Shutdown();
            AwaitCondition(() => shutdown.IsCompleted);
        }

        [Fact]
        public async Task PlainSource_should_fail_stage_if_broker_unavailable()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            var config = ConsumerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers("localhost:10092")
                .WithGroupId(group1);

            var (control, probe) = CreateProbe(config, Subscriptions.Assignment(topicPartition1));
            probe.Request(1);
            AwaitCondition(() => control.IsShutdown.IsCompleted, TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task PlainSource_should_stop_on_deserialization_errors()
        {
            int elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.StoppingDecider))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            var error = probe.Request(elementsCount).ExpectEvent(TimeSpan.FromSeconds(10));
            error.Should().BeOfType<TestSubscriber.OnError>();
            ((TestSubscriber.OnError)error).Cause.Should().BeOfType<SerializationException>();
            probe.Cancel();
        }

        [Fact]
        public async Task PlainSource_should_resume_on_deserialization_errors()
        {
            Directive Decider(Exception cause) => cause is SerializationException
                ? Directive.Resume
                : Directive.Stop;

            int elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            probe.Request(elementsCount);
            probe.ExpectNoMsg(TimeSpan.FromSeconds(10));
            probe.Cancel();
        }
    }
}
