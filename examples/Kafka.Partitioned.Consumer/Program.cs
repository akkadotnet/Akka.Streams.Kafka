
using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using Kafka.Partitioned.Consumer.Actors;

namespace Kafka.Partitioned.Consumer
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = INFO
                ").WithFallback(KafkaExtensions.DefaultSettings);

            var system = ActorSystem.Create("TestKafka", config);
            var consumerSettings = ConsumerSettings<string, string>.Create(system, null, null)
                .WithBootstrapServers("localhost:29092")
                .WithGroupId("group1");
            var subscription = Subscriptions.Topics("akka100");
            system.ActorOf(KafkaConsumerSupervisor<string, string>.Props(consumerSettings, subscription, 3), "kafka");

            Console.WriteLine("Press any key to stop consumer.");
            Console.ReadKey();

            await system.Terminate();
        }
    }
}
