// -----------------------------------------------------------------------
//  <copyright file="CommittableSourceBenchmark.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
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
public class CommittableSourceBenchmark : KafkaConsumerBenchmark<int>
{
    [Params(500)] public int PollBatchSize { get; set; }

    [Params(1000)] public int CommitBatchSize { get; set; }

    protected override Source<int, IControl> CreateSource()
    {
        var consumerSettings = CreateConsumerSettings<Null, string>()
            .WithMaxPollRecords(PollBatchSize);
        var committerSettings = CommitterSettings.Create(ActorSystem!)
            .WithMaxBatch(CommitBatchSize)
            // don't let a final partial batch wait for the default 10s max-interval
            // (100k is a multiple of CommitBatchSize today, but keep this in line with the partitioned benchmark)
            .WithMaxInterval(TimeSpan.FromMilliseconds(100));

        return KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(TopicName))
            .Select(ICommittable (message) => message.CommitableOffset)
            .Via(Committer.BatchFlow(committerSettings))
            // have to add this to make sure the `Take` stage gets what it needs
            .SelectMany(c => new int[c.BatchSize])
            .Select(c => c);
    }

    [Benchmark(OperationsPerInvoke = TestMessageCount)]
    [BenchmarkCategory(BenchmarkCategories.MacroBenchmark, BenchmarkCategories.ConsumerBenchmark,
        BenchmarkCategories.CommittableConsumerBenchmark)]
    public Task ConsumeMessageAsync()
    {
        StartDemand();
        return CompletionTask!;
    }
}