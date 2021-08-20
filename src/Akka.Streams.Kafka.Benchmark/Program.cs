using System;
using BenchmarkDotNet.Running;

namespace Akka.Streams.Kafka.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<PlainSinkConsumerBenchmark>();
        }
    }
}