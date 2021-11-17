using System;
using System.Reflection;
using BenchmarkDotNet.Running;

namespace Akka.Streams.Kafka.Benchmark
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(Assembly.GetExecutingAssembly()).Run(args);
        }
    }
}