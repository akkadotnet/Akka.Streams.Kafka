using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [MinWarmupCount(3)]
    [MaxWarmupCount(5)]
    [MinIterationCount(3)]
    [MaxIterationCount(5)]
    public class BatchCommitBenchmark : KafkaConsumerBenchmark<ICommittableOffsetBatch>
    {
        [Params(10, 100, 1000)]
        public int BatchSize { get; set; }

        protected override Source<ICommittableOffsetBatch, IControl> CreateSource()
        {
            var consumerSettings = CreateConsumerSettings<Null, string>();
            var committerSettings = CommitterSettings.Create(ActorSystem)
                .WithMaxBatch(BatchSize);

            return KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(TopicName))
                .Select(ICommittable (message) => message.CommitableOffset)
                .Via(Committer.BatchFlow(committerSettings));
        }
    }
} 