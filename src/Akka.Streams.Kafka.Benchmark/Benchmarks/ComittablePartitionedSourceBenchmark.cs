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

namespace Akka.Streams.Kafka.Benchmark.Benchmarks;

[Config(typeof(MacroBenchmarkConfig))]
public class CommittablePartitionedSourceBenchmark : KafkaConsumerBenchmark<Offset>
{
    [Params(500)] public int PollBatchSize { get; set; }

    [Params(1000)] public int CommitBatchSize { get; set; }


    protected override Source<Offset, IControl> CreateSource()
    {
        var mergeHubSource = MergeHub.Source<ICommittableOffsetBatch>(perProducerBufferSize: 10);
        var (sink, trueSource) = mergeHubSource.PreMaterialize(ActorSystem);

        var consumerSettings = CreateConsumerSettings<Null, string>()
            .WithMaxPollRecords(PollBatchSize);
        var committerSettings = CommitterSettings.Create(ActorSystem!)
            .WithMaxBatch(CommitBatchSize);

        var (control, partitionedSrc) = KafkaConsumer
            .CommittablePartitionedSource(consumerSettings, Subscriptions.Topics(TopicName))
            .Select(tup =>
            {
                var (partition, src) = tup;

                return src
                    .Select(ICommittable (message) => message.CommitableOffset)
                    .Via(Committer.BatchFlow(committerSettings)).RunWith(sink, ActorSystem);
            })
            .PreMaterialize(ActorSystem);

        partitionedSrc.RunWith(Sink.Ignore<NotUsed>(), ActorSystem);

        return trueSource.SelectMany(c => c.Offsets)
            .Select(c => c.Offset)
            .MapMaterializedValue(_ => control);
    }

    [Benchmark(OperationsPerInvoke = TestMessageCount)]
    public Task ConsumeMessageAsync()
    {
        StartDemand();
        return CompletionTask!;
    }
}