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
using Akka.Streams.Kafka.Testkit.Fixture;
using Akka.Streams.TestKit;
using Akka.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class ExternalPlainSourceIntegrationTests : KafkaIntegrationTests
    {
        public ExternalPlainSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(ExternalPlainSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task ExternalPlainSource_with_external_consumer_Should_work()
        {
            var elementsCount = 10;
            var topic = CreateTopicName(1);
            var group = CreateGroupId(1);
            
            //Consumer is represented by actor
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(CreateConsumerSettings<string>(group)));
            
            //Manually assign topic partition to it
            var (control1, probe1) = CreateExternalPlainSourceProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (control2, probe2) = CreateExternalPlainSourceProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            
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
            AwaitCondition(() => control1.IsShutdown.IsCompletedSuccessfully && control2.IsShutdown.IsCompletedSuccessfully);
            
            // Cleanup
            consumer.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, ActorRefs.NoSender);
        }

        [Fact]
        public async Task ExternalPlainSource_should_be_stopped_on_serialization_error_only_when_requested_messages()
        {
            var topic = CreateTopicName(1);
            var group = CreateGroupId(1);

            // Make consumer expect numeric messages
            var settings = CreateConsumerSettings<int>(group).WithValueDeserializer(Deserializers.Int32);
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(settings));
            
            // Subscribe to partitions
            var (control1, probe1) = CreateExternalPlainSourceProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (control2, probe2) = CreateExternalPlainSourceProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            var (control3, probe3) = CreateExternalPlainSourceProbe<int>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 2)));

            // request from 2 streams
            probe1.Request(1);
            probe2.Request(1);
            await Task.Delay(500); // To establish demand

            // Send string messages
            await ProduceStrings(new TopicPartition(topic, 0), new int[] { 1 }, ProducerSettings);
            await ProduceStrings(new TopicPartition(topic, 1), new int[] { 1 }, ProducerSettings);

            // First two stages should fail, and only stage without demand should keep going
            var ex = probe1.ExpectError();
            ex.Should().BeOfType<ConsumeException>();
            ((ConsumeException) ex).Error.IsSerializationError().Should().BeTrue();
            
            ex = probe2.ExpectError();
            ex.Should().BeOfType<ConsumeException>();
            ((ConsumeException) ex).Error.IsSerializationError().Should().BeTrue();
            
            probe3.Cancel();
            
            // Make sure source tasks finish accordingly
            AwaitCondition(() => control1.IsShutdown.IsCompleted && control2.IsShutdown.IsCompleted && control3.IsShutdown.IsCompletedSuccessfully);
            
            // Cleanup
            consumer.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, ActorRefs.NoSender);
        }

        [Fact]
        public async Task ExternalPlainSource_verify_consuming_actor_pause_resume_partitions_works_fine()
        {
            var topic = CreateTopicName(1);
            var group = CreateGroupId(1);

            // Create consumer actor
            var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(CreateConsumerSettings<string>(group)));
            
            // Send one message per each partition
            await ProduceStrings(new TopicPartition(topic, 0), Enumerable.Range(1, 100), ProducerSettings);
            await ProduceStrings(new TopicPartition(topic, 1), Enumerable.Range(1, 100), ProducerSettings);
            
            // Subscribe to partitions
            var (control1, probe1) = CreateExternalPlainSourceProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 0)));
            var (control2, probe2) = CreateExternalPlainSourceProbe<string>(consumer, Subscriptions.Assignment(new TopicPartition(topic, 1)));
            
            var probes = new[] { probe1, probe2 };
            
            // All partitions resumed
            probes.ForEach(p => p.Request(1));
            probes.ForEach(p => p.ExpectNext(TimeSpan.FromSeconds(10)));

            await Task.Delay(1000); // All partitions become paused when now demand

            // Make resumed and second paused
            probe1.Request(1);
            probe1.ExpectNext(TimeSpan.FromSeconds(10)); 
            
            await Task.Delay(1000); // All partitions become paused when now demand
            
            // Make second resumed and first paused
            probe2.Request(1);
            probe2.ExpectNext(TimeSpan.FromSeconds(10)); 
            
            await Task.Delay(1000); // All partitions become paused when now demand
            
            // All partitions resumed back
            probes.ForEach(p => p.Request(1));
            probes.ForEach(p => p.ExpectNext(TimeSpan.FromSeconds(10)));
            
            // Stop and check gracefull shutdown
            probes.ForEach(p => p.Cancel());
            AwaitCondition(() => control1.IsShutdown.IsCompletedSuccessfully && control2.IsShutdown.IsCompletedSuccessfully);
            
            // Cleanup
            consumer.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, ActorRefs.NoSender);
        }
    }
}