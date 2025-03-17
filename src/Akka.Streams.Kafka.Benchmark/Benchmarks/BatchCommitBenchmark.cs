using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Configs;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [Config(typeof(MacroBenchmarkConfig))]
    public class BatchCommitBenchmark : KafkaConsumerBenchmark<ICommittableOffsetBatch>
    {
        [Params(10, 100, 500)]
        public int PollBatchSize { get; set; }
        
        [Params(10, 100, 500)]
        public int CommitBatchSize { get; set; }

        protected override Source<ICommittableOffsetBatch, IControl> CreateSource()
        {
            var consumerSettings = CreateConsumerSettings<Null, string>()
                .WithMaxPollRecords(PollBatchSize);
            var committerSettings = CommitterSettings.Create(ActorSystem!)
                .WithMaxBatch(CommitBatchSize);

            return KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(TopicName))
                .Select(ICommittable (message) => message.CommitableOffset)
                .Via(Committer.BatchFlow(committerSettings));
        }
        
        [Benchmark(OperationsPerInvoke = TestMessageCount)]
        public Task ConsumeMessageAsync()
        {
            StartDemand();
            return CompletionTask!;
        }
    }
} 