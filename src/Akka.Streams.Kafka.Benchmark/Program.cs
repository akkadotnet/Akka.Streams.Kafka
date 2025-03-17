using System;
using System.Reflection;
using BenchmarkDotNet.Running;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Akka.Streams.Kafka Benchmarks");
            Console.WriteLine("----------------------------");
            Console.WriteLine("IMPORTANT: Make sure Kafka is running via 'docker-compose up -d' before running benchmarks.");
            Console.WriteLine("See README.md for detailed instructions.");
            Console.WriteLine();
            
            // Try to connect to Kafka to give early warning
            try
            {
                using var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = "localhost:29092",
                    SocketTimeoutMs = 5000
                }).Build();
                
                adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            }
            catch (Exception)
            {
                Console.WriteLine("ERROR: Could not connect to Kafka at localhost:29092");
                Console.WriteLine("Please make sure Kafka is running via 'docker-compose up -d'");
                return;
            }
            
            BenchmarkSwitcher.FromAssembly(Assembly.GetExecutingAssembly()).Run(args);
        }
    }
}