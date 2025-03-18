using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Streams.Kafka.Helpers;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class CommitterFlowIntegrationTests : KafkaIntegrationTests
    {
        public CommitterFlowIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(CommitterFlowIntegrationTests), output, fixture)
        {
        }

        [Theory(Skip = "Looks fishy")]
        [InlineData(1)]
        [InlineData(5)]
        public async Task CommitterFlow_commits_offsets_from_CommittableSource(int batchSize)
        {
            var topic1 = CreateTopic(1);
            var topicPartition1 = new TopicPartition(topic1, 0);
            var group1 = CreateGroup(1);

            await GivenInitializedTopicAsync(topicPartition1);

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition1, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);

            var consumerSettings = CreateConsumerSettings<string>(group1);
            var committedElements = new ConcurrentQueue<string>();
            var committerSettings = CommitterSettings.WithMaxBatch(batchSize);
            
            var (task, probe1) = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Assignment(topicPartition1))
                .SelectAsync(10, elem =>
                {
                    committedElements.Enqueue(elem.Record.Message.Value);
                    return Task.FromResult<ICommittable>(elem.CommitableOffset);
                })
                .Via(Committer.Flow(committerSettings))
                .ToMaterialized(this.SinkProbe<Done>(), Keep.Both)
                .Run(Materializer);

            await probe1.RequestAsync(25 / batchSize);

            foreach (var _ in Enumerable.Range(1, 25 / batchSize))
            {
                await probe1.ExpectNextAsync(Done.Instance, TimeSpan.FromSeconds(10));
            }
                
            probe1.Cancel();

            await AwaitConditionAsync(() => task.IsShutdown.IsCompletedSuccessfully);

            var probe2 = KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Select(r => r.Message.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            await probe2.RequestAsync(75);
            foreach (var i in Enumerable.Range(committedElements.Count + 1, 75).Select(c => c.ToString()))
                probe2.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe2.Cancel();
        }
    }
}
