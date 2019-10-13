using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
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

            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);
            
            var probe = KafkaConsumer.PlainPartitionedManualOffsetSource(
                consumerSettings, 
                Subscriptions.Topics(topic), 
                getOffsetsOnAssign: _ => Task.FromResult(ImmutableHashSet<TopicPartitionOffset>.Empty as IImmutableSet<TopicPartitionOffset>),
                onRevoke: _ => { }
                ).MergeMany(3, tuple => tuple.Item2.MapMaterializedValue(notUsed => new NoopControl()))
                .Select(m => m.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(totalMessages);
            var messages = probe.Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(totalMessages));
            messages.Should().BeEquivalentTo(Enumerable.Range(1, totalMessages).Select(m => m.ToString()));
            
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
    }
}