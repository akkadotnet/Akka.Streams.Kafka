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