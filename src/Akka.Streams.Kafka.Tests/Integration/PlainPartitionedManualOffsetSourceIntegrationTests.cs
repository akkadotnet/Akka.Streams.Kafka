using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Streams.Util;
using Akka.Util;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class PlainPartitionedManualOffsetSourceIntegrationTests : KafkaIntegrationTests
    {
        public PlainPartitionedManualOffsetSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(PlainPartitionedManualOffsetSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task PlainPartitionedManualOffsetSource_Should_begin_consuming_from_beginning_of_the_topic()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;
            var consumerSettings = CreateConsumerSettings<string>(group);

            var allMessages = Enumerable.Range(1, totalMessages).ToList();

            await ProduceStrings(topic, allMessages, ProducerSettings);
            
            var probe = KafkaConsumer.PlainPartitionedManualOffsetSource(
                consumerSettings, 
                Subscriptions.Topics(topic), 
                getOffsetsOnAssign: _ => Task.FromResult(ImmutableHashSet<TopicPartitionOffset>.Empty as IImmutableSet<TopicPartitionOffset>),
                onRevoke: _ => { }
                ).MergeMany(3, tuple => tuple.Item2.MapMaterializedValue(notUsed => new NoopControl()))
                .Select(m => m.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(totalMessages);
            probe.Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(totalMessages));
            probe.Cancel();
        }
        
        [Fact]
        public async Task PlainPartitionedManualOffsetSource_Should_begin_consuming_with_offset()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var consumerSettings = CreateConsumerSettings<string>(group);

            await ProduceStrings(topic, Enumerable.Range(1, 100), ProducerSettings);
            
            var probe = KafkaConsumer.PlainPartitionedManualOffsetSource(
                    consumerSettings, 
                    Subscriptions.Topics(topic), 
                    getOffsetsOnAssign: topicPartitions =>
                    {
                        // Skip first message from first partition
                        var firstPartition = topicPartitions.OrderBy(tp => tp.Partition.Value).First();
                        var offset = ImmutableHashSet<TopicPartitionOffset>.Empty.Add(new TopicPartitionOffset(firstPartition, 1));
                        return Task.FromResult<IImmutableSet<TopicPartitionOffset>>(offset);
                    },
                    onRevoke: _ => { }
                ).MergeMany(3, tuple => tuple.Item2.MapMaterializedValue(notUsed => new NoopControl()))
                .Select(m => m.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(99);
            var messages = probe.Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(99));
            messages.ToHashSet().Count.Should().Be(99); // All consumed messages should be different (only one value is missing)
            
            probe.Cancel();
        }
        
        [Fact]
        public async Task PlainPartitionedManualOffsetSource_Should_call_the_OnRevoke_hook()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var consumerSettings = CreateConsumerSettings<string>(group);

            var partitionsAssigned = false;
            var revoked = Option<IImmutableSet<TopicPartition>>.None;
            
            // Create topic to allow consumer assignment
            await ProduceStrings(topic, new []{ 0 }, ProducerSettings);
            
            var source = KafkaConsumer.PlainPartitionedManualOffsetSource(consumerSettings, Subscriptions.Topics(topic),
                assignedPartitions =>
                {
                    partitionsAssigned = true;
                    return Task.FromResult(ImmutableHashSet<TopicPartitionOffset>.Empty as IImmutableSet<TopicPartitionOffset>);
                },
                revokedPartitions =>
                {
                    revoked = new Option<IImmutableSet<TopicPartition>>(revokedPartitions);
                })
                .MergeMany(3, tuple => tuple.Item2.MapMaterializedValue(notUsed => new NoopControl()))
                .Select(m => m.Value);
            
            var (control1, firstConsumer) = source.ToMaterialized(this.SinkProbe<string>(), Keep.Both).Run(Materializer);
            
            AwaitCondition(() => partitionsAssigned, TimeSpan.FromSeconds(10), "First consumer should get asked for offsets");

            var secondConsumer = source.RunWith(this.SinkProbe<string>(), Materializer);
            
            AwaitCondition(() => revoked.Value?.Count > 0, TimeSpan.FromSeconds(10));

            firstConsumer.Cancel();
            secondConsumer.Cancel();
            AwaitCondition(() => control1.IsShutdown.IsCompletedSuccessfully, TimeSpan.FromSeconds(10));
        }
    }
}