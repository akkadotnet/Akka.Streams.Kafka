// -----------------------------------------------------------------------
//  <copyright file="ComittablePartitionedSourceBenchmark.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

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

namespace Akka.Streams.Kafka.Benchmark;

[Config(typeof(MacroBenchmarkConfig))]
public class CommittablePartitionedSourceBenchmark : KafkaConsumerBenchmark<int>
{
    [Params(500)] public int PollBatchSize { get; set; }

    [Params(1000)] public int CommitBatchSize { get; set; }


    protected override Source<int, IControl> CreateSource()
    {
        var mergeHubSource = MergeHub.Source<ICommittableOffsetBatch>(10);
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

        return trueSource.SelectMany(c => new int[c.Offsets.Count])
            .Select(c => c)
            .MapMaterializedValue(_ => control);
    }

    [Benchmark(OperationsPerInvoke = TestMessageCount)]
    [BenchmarkCategory(BenchmarkCategories.MacroBenchmark, BenchmarkCategories.ConsumerBenchmark,
        BenchmarkCategories.PartitionedConsumerBenchmark, BenchmarkCategories.CommittableConsumerBenchmark)]
    public Task ConsumeMessageAsync()
    {
        StartDemand();
        return CompletionTask!;
    }
}