using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Configs;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [Config(typeof(MacroBenchmarkConfig))]
    public class PlainSourceBenchmark : KafkaConsumerBenchmark<ConsumeResult<Null, string>>
    {
        [Params(10, 100, 500)]
        public int PollBatchSize { get; set; }
        
        protected override Source<ConsumeResult<Null, string>, IControl> CreateSource()
        {
            var consumerSettings = CreateConsumerSettings<Null, string>()
                .WithMaxPollRecords(PollBatchSize);
            return KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(TopicName));
        }
        
        [Benchmark(OperationsPerInvoke = TestMessageCount)]
        public Task ConsumeMessageAsync()
        {
            StartDemand();
            return CompletionTask!;
        }
    }
} 