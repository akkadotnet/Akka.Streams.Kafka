using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [MinWarmupCount(3)]
    [MaxWarmupCount(5)]
    [MinIterationCount(3)]
    [MaxIterationCount(5)]
    public class PlainConsumerBenchmark : KafkaConsumerBenchmark<ConsumeResult<Null, string>>
    {
        protected override Source<ConsumeResult<Null, string>, IControl> CreateSource()
        {
            var consumerSettings = CreateConsumerSettings<Null, string>();
            return KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(TopicName));
        }
    }
} 