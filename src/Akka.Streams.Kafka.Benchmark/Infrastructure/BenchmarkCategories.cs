// -----------------------------------------------------------------------
//  <copyright file="BenchmarkCategories.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public static class BenchmarkCategories
{
    public const string ConsumerBenchmark = "Consumer";
    public const string PartitionedConsumerBenchmark = "Partitioned";
    public const string CommittableConsumerBenchmark = "Committable";
    public const string PlainConsumerBenchmark = "Plain";
    public const string ProducerBenchmark = "Producer";

    public const string MacroBenchmark = "MacroBenchmark";
    public const string MicroBenchmark = "MicroBenchmark";
}