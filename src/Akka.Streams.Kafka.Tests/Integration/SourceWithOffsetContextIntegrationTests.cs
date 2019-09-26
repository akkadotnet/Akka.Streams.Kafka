using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class SourceWithOffsetContextIntegrationTests : KafkaIntegrationTests
    {
        public SourceWithOffsetContextIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(SourceWithOffsetContextIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task SourceWithOffsetContext_at_least_once_consuming_should_work()
        {
            var topic = CreateTopic(1);
            var settings = CreateConsumerSettings<string>(CreateGroup(1));
            var messages = Enumerable.Range(1, 10).ToList();

            await ProduceStrings(topic, messages, ProducerSettings);
            
            var (task, probe) = KafkaConsumer.SourceWithOffsetContext(settings, Subscriptions.Topics(topic))
                .SelectAsync(10, message => Task.FromResult(Done.Instance))
                .Via(Committer.FlowWithOffsetContext<Done>(CommitterSettings))
                .AsSource()
                .ToMaterialized(this.SinkProbe<Tuple<NotUsed, ICommittableOffsetBatch>>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);
            var committedBatches = probe.Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(10));

            probe.Cancel();
            
            AwaitCondition(() => task.IsCompletedSuccessfully, TimeSpan.FromSeconds(10));

            committedBatches.Select(r => r.Item2).Sum(batch => batch.BatchSize).Should().Be(10);
        }
    }
}