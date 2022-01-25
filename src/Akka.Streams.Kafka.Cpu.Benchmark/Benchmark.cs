using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Cpu.Benchmark
{
    public static class Benchmark
    {
        public static ActorSystem ConsumerSystem { get; private set; }
        public static string KafkaTopic { get; }
        public static string KafkaGroup { get; }
        public static readonly DockerSupport Docker;
        
        static Benchmark()
        {
            Docker = new DockerSupport();
            
            var uuid = Guid.NewGuid().ToString();
            KafkaTopic = $"topic-1-{uuid}";
            KafkaGroup = $"group-1-{uuid}";
        }
        
        public static async Task SetupKafkaAsync()
        {
            await Docker.SetupContainersAsync();
            await Docker.WaitForKafkaServerAsync();
        }

        public static async Task SetupAkkaAsync()
        {
            Console.WriteLine("Starting Akka ActorSystems");
            
            var config = ConfigurationFactory.ParseString(@"
akka {
    log-config-on-start = off
    stdout-loglevel = INFO
    loglevel = INFO
    actor {
        debug {
            receive = on
            autoreceive = on
            lifecycle = on
            event-stream = on
            unhandled = on
        }
    }
}")
                .WithFallback(KafkaExtensions.DefaultSettings);
            
            ConsumerSystem = ActorSystem.Create("akka-kafka-consumer", config);
            Console.WriteLine("ActorSystems started");
        }

        public static async Task TearDownAkkaAsync()
        {
            try
            {
                await ConsumerSystem.Terminate();
            }
            catch
            {
                // no-op
            }
        }
        
        public static async Task TearDownKafkaAsync()
        {
            await Docker.TearDownDockerAsync();
        }

    }
}