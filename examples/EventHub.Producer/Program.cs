using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace EventHub.Producer
{
    class Program
    {
        private const string EventHubNamespace = "YOUR AZURE EVENT HUB NAMESPACE";
        private const string EventHubConnectionString = "YOUR AZURE EVENT HUB CONNECTION STRING";
        private const string EventHubName = "YOUR AZURE EVENT HUB NAME";

        static async Task Main(string[] args)
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var producerSettings = ProducerSettings<Null, string>.Create(system, null, null)
                .WithBootstrapServers($"{EventHubNamespace}.servicebus.windows.net:9093")
                .WithProperties(new Dictionary<string, string>
                {
                    {"security.protocol", "SASL_SSL"},
                    {"sasl.mechanism", "PLAIN"},
                    {"sasl.username", "$ConnectionString"},
                    {"sasl.password", EventHubConnectionString},
                });

            await Source.From(Enumerable.Range(1, 100))
                .Select(c => c.ToString())
                .Select(elem => ProducerMessage.Single(new ProducerRecord<Null, string>(EventHubName, elem)))
                .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(producerSettings))
                .Select(result =>
                {
                    var response = result as Result<Null, string, NotUsed>;
                    Console.WriteLine($"Producer: {response.Metadata.Topic}/{response.Metadata.Partition} {response.Metadata.Offset}: {response.Metadata.Value}");
                    return result;
                })
                .RunWith(Sink.Ignore<IResults<Null, string, NotUsed>>(), materializer);

            Console.ReadKey();

            await system.Terminate();
        }
    }
}
