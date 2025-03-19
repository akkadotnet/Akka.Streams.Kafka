using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Configs;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark;

[Config(typeof(MacroBenchmarkConfig))]
public class PlainPartitionedSourceBenchmark : KafkaConsumerBenchmark<ConsumeResult<Null, string>>
{
    [Params(500)] public int PollBatchSize { get; set; }

    protected override Source<ConsumeResult<Null, string>, IControl> CreateSource()
    {
        var mergeHubSource = MergeHub.Source<ConsumeResult<Null, string>>(perProducerBufferSize:10);
        var (sink, trueSource) = mergeHubSource.PreMaterialize(ActorSystem);

        var consumerSettings = CreateConsumerSettings<Null, string>()
            .WithMaxPollRecords(PollBatchSize);

        var (control, partitionedSrc) = KafkaConsumer
            .PlainPartitionedSource(consumerSettings, Subscriptions.Topics(TopicName))
            .Select(tup =>
            {
                var (partition, src) = tup;
                return src.RunWith(sink, ActorSystem);
            })
            .PreMaterialize(ActorSystem);

        partitionedSrc.RunWith(Sink.Ignore<NotUsed>(), ActorSystem);

        return trueSource.Select(c =>
        {
            
            return c;
        }).MapMaterializedValue(_ => control);
    }

    [Benchmark(OperationsPerInvoke = TestMessageCount)]
    [BenchmarkCategory(BenchmarkCategories.MacroBenchmark, BenchmarkCategories.ConsumerBenchmark,
        BenchmarkCategories.PlainConsumerBenchmark, BenchmarkCategories.PlainConsumerBenchmark)]
    public Task ConsumeMessageAsync()
    {
        StartDemand();
        return CompletionTask!;
    }
}