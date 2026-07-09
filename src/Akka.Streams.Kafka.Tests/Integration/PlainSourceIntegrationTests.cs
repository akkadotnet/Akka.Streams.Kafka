// -----------------------------------------------------------------------
//  <copyright file="PlainSourceIntegrationTests.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Integration;

public class PlainSourceIntegrationTests : KafkaIntegrationTests
{
    public PlainSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(PlainSourceIntegrationTests), output, fixture)
    {
    }


    private (IControl, TestSubscriber.Probe<string>) CreateProbe(ConsumerSettings<Null, string> consumerSettings,
        ISubscription sub) =>
        KafkaConsumer
            .PlainSource(consumerSettings, sub)
            .Select(c => c.Message.Value)
            .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
            .Run(Materializer);

    [Fact]
    public async Task PlainSource_consumes_messages_from_KafkaProducer_with_topicPartition_assignment()
    {
        var elementsCount = 100;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var topicPartition1 = new TopicPartition(topic1, 0);

        await GivenInitializedTopicAsync(topicPartition1);

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
        var elementsCount = 100;
        var offset = 50;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var topicPartition1 = new TopicPartition(topic1, 0);

        await GivenInitializedTopicAsync(topicPartition1);

        await ProduceStrings(topicPartition1, Enumerable.Range(0, elementsCount), ProducerSettings);

        var consumerSettings = CreateConsumerSettings<string>(group1);

        var (_, probe) = CreateProbe(consumerSettings,
            Subscriptions.AssignmentWithOffset(new TopicPartitionOffset(topicPartition1, new Offset(offset))));

        probe.Request(elementsCount);
        foreach (var i in Enumerable.Range(offset, elementsCount - offset).Select(c => c.ToString()))
            probe.ExpectNext(i, TimeSpan.FromSeconds(10));

        probe.Cancel();
    }

    [Fact]
    public async Task PlainSource_consumes_messages_from_KafkaProducer_with_subscribe_to_topic()
    {
        var elementsCount = 100;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var topicPartition1 = new TopicPartition(topic1, 0);

        await GivenInitializedTopicAsync(topicPartition1);

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
    public async Task PlainSource_should_resume_stage_if_broker_unavailable()
    {
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var topicPartition1 = new TopicPartition(topic1, 0);

        await GivenInitializedTopicAsync(topicPartition1);

        var config = ConsumerSettings<Null, string>.Create(Sys, null, null)
            .WithBootstrapServers("localhost:10092")
            .WithGroupId(group1);

        var regex = new Regex("\\[localhost:10092\\/bootstrap: Connect to [a-zA-Z0-9#:.*]* failed:");
        var logProbe = CreateTestProbe();
        Sys.EventStream.Subscribe<Info>(logProbe.Ref);

        var (control, probe) = CreateProbe(config, Subscriptions.Assignment(topicPartition1));
        probe.Request(1);

        AwaitAssert(() =>
        {
            var info = logProbe.ExpectMsg<Info>();
            Assert.Matches(regex, info.Message.ToString());
            Assert.Contains("[Resume]", info.Message.ToString());
        });
        //AwaitCondition(() => control.IsShutdown.IsCompleted, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task PlainSource_should_stop_on_deserialization_errors()
    {
        var elementsCount = 10;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);

        await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

        var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

        var probe = KafkaConsumer
            .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.StoppingDecider))
            .Select(c => c.Message.Value)
            .RunWith(this.SinkProbe<int>(), Materializer);

        var @event = probe.Request(elementsCount).ExpectEvent(TimeSpan.FromSeconds(10));
        var error = (TestSubscriber.OnError)@event;
        var exception = (ConsumeException)error.Cause;
        Assert.Equal(ErrorCode.Local_ValueDeserialization, exception.Error.Code);
        probe.Cancel();
    }

    [Fact]
    public async Task PlainSource_with_directive_override_should_resume_on_deserialization_errors()
    {
        var callCount = 0;

        Directive Decider(Exception cause)
        {
            if (cause is ConsumeException ex && ex.Error.IsSerializationError())
            {
                callCount++;
                return Directive.Resume;
            }

            return Directive.Stop;
        }

        var elementsCount = 10;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);

        await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

        var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

        var probe = KafkaConsumer
            .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
            .Select(c => c.Message.Value)
            .RunWith(this.SinkProbe<int>(), Materializer);

        probe.Request(elementsCount);
        probe.ExpectNoMsg(TimeSpan.FromSeconds(10));
        // this is twice elementCount because Decider is called twice on each exceptions
        Assert.Equal(elementsCount * 2, callCount);
        probe.Cancel();
    }

    [Fact]
    public async Task Custom_partition_event_handling_Should_work()
    {
        var elementsCount = 100;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var topicPartition1 = new TopicPartition(topic1, 0);

        await GivenInitializedTopicAsync(topicPartition1);

        await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

        var consumerSettings = CreateConsumerSettings<string>(group1);

        var customHandler = new CustomEventsHandler();
        var (control, probe) = CreateProbe(consumerSettings,
            Subscriptions.Topics(topic1).WithPartitionEventsHandler(customHandler));

        probe.Request(elementsCount);
        foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
            probe.ExpectNext(i, TimeSpan.FromSeconds(10));

        var shutdown = control.Shutdown();
        await AwaitConditionAsync(() => shutdown.IsCompleted);

        Assert.True((customHandler.AssignmentEventsCounter.Current) > (0));
        Assert.True((customHandler.StopEventsCounter.Current) > (0));
    }

    private class CustomEventsHandler : IPartitionEventHandler
    {
        public AtomicCounter AssignmentEventsCounter = new(0);
        public AtomicCounter RevokeEventsCounter = new(0);
        public AtomicCounter StopEventsCounter = new(0);


        /// <inheritdoc />
        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
            IRestrictedConsumer consumer) =>
            RevokeEventsCounter.IncrementAndGet();

        /// <inheritdoc />
        public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
            IRestrictedConsumer consumer) =>
            RevokeEventsCounter.IncrementAndGet();

        /// <inheritdoc />
        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer) =>
            AssignmentEventsCounter.IncrementAndGet();

        /// <inheritdoc />
        public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer) =>
            StopEventsCounter.IncrementAndGet();
    }
}