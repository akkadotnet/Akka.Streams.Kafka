using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
using Akka.Streams.Kafka.Testkit.Fixture;
using Akka.Streams.Kafka.Tests.Logging;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

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
                .Select(c => c.Message.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);
        }

        [Fact]
        public async Task PlainSource_consumes_messages_from_KafkaProducer_with_topicPartition_assignment()
        {
            int elementsCount = 100;
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);
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
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            await ProduceStrings(topicPartition1, Enumerable.Range(0, elementsCount), ProducerSettings);

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
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);
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
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);
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
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.StoppingDecider))
                .Select(c => c.Message.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            var error = probe.Request(elementsCount).ExpectEvent(TimeSpan.FromSeconds(10));
            error.Should().BeOfType<TestSubscriber.OnError>();
            ((TestSubscriber.OnError)error).Cause.Should().BeOfType<ConsumeException>();
            probe.Cancel();
        }

        [Fact]
        public async Task PlainSource_should_resume_on_deserialization_errors()
        {
            var callCount = 0;
            Directive Decider(Exception cause)
            {
                if(cause is ConsumeException ex && ex.Error.IsSerializationError())
                {
                    callCount++;
                    return Directive.Resume;
                }
                return Directive.Stop;
            }

            int elementsCount = 10;
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .Select(c => c.Message.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            probe.Request(elementsCount);
            probe.ExpectNoMsg(TimeSpan.FromSeconds(10));
            callCount.Should().Be(elementsCount);
            probe.Cancel();
        }

        [Fact]
        public async Task Custom_partition_event_handling_Should_work()
        {
            int elementsCount = 100;
            var topic1 = CreateTopicName(1);
            var group1 = CreateGroupId(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group1);

            var customHandler = new CustomEventsHandler();
            var (control, probe) = CreateProbe(consumerSettings, Subscriptions.Topics(topic1).WithPartitionEventsHandler(customHandler));

            probe.Request(elementsCount);
            foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
                probe.ExpectNext(i, TimeSpan.FromSeconds(10));

            var shutdown = control.Shutdown();
            await AwaitConditionAsync(() => shutdown.IsCompleted);

            customHandler.AssignmentEventsCounter.Current.Should().BeGreaterThan(0);
            customHandler.StopEventsCounter.Current.Should().BeGreaterThan(0);
        }

        class CustomEventsHandler : IPartitionEventHandler
        {
            public AtomicCounter AssignmentEventsCounter = new AtomicCounter(0);
            public AtomicCounter RevokeEventsCounter = new AtomicCounter(0);
            public AtomicCounter StopEventsCounter = new AtomicCounter(0);


            /// <inheritdoc />
            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
                IRestrictedConsumer consumer)
            {
                RevokeEventsCounter.IncrementAndGet();
            }

            /// <inheritdoc />
            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
                AssignmentEventsCounter.IncrementAndGet();
            }

            /// <inheritdoc />
            public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
            {
                StopEventsCounter.IncrementAndGet();
            }
        }
    }
}
