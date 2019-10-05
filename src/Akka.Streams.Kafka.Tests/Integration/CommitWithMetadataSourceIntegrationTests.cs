using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    /// <summary>
    /// CommitWithMetadataSourceIntegrationTests
    /// </summary>
    public class CommitWithMetadataSourceIntegrationTests : KafkaIntegrationTests
    {
        /// <summary>
        /// CommitWithMetadataSourceIntegrationTests
        /// </summary>
        public CommitWithMetadataSourceIntegrationTests( ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(CommitWithMetadataSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task CommitWithMetadataSource_Commit_metadata_in_message_Should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            
            string MetadataFromMessage<K, V>(ConsumeResult<K, V> message) => message.Offset.ToString();

            await ProduceStrings(topic, Enumerable.Range(1, 10), ProducerSettings);
            
            var (task, probe) = KafkaConsumer.CommitWithMetadataSource(CreateConsumerSettings<string>(group), Subscriptions.Topics(topic), MetadataFromMessage)
                .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);
            
            probe.Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(10)).ForEach(message =>
            {
                var offsetWithMeta = message.CommitableOffset as ICommittableOffsetMetadata;
                offsetWithMeta.Should().NotBeNull();
                offsetWithMeta.Metadata.Should().Be(message.CommitableOffset.Offset.Offset.ToString());
            });

            probe.Cancel();
            
            AwaitCondition(() => task.IsCompletedSuccessfully, TimeSpan.FromSeconds(10));
        }
    }
}