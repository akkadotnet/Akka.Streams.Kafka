using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace EventHub.Consumer
{
    class Program
    {
        private const string EventHubNamespace = "YOUR AZURE EVENT HUB NAMESPACE";
        private const string EventHubConnectionString = "YOUR AZURE EVENT HUB CONNECTION STRING";
        private const string EventHubName = "YOUR AZURE EVENT HUB NAME";
        private const string EventHubConsumerGroup = "$Default";

        public static async Task Main(string[] args)
        {

            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, null)
                .WithBootstrapServers($"{EventHubNamespace}.servicebus.windows.net:9093")
                .WithGroupId(EventHubConsumerGroup)
                .WithProperties(new Dictionary<string, string>
                {
                    {"security.protocol", "SASL_SSL"},
                    {"sasl.mechanism", "PLAIN"},
                    {"sasl.username", "$ConnectionString"},
                    {"sasl.password", EventHubConnectionString},
                });

            var subscription = Subscriptions.Topics(EventHubName);

            var committerDefaults = CommitterSettings.Create(system);

            // Comment for simple no-commit consumer
            DrainingControl<NotUsed> control = KafkaConsumer.CommittableSource(consumerSettings, subscription)
                .SelectAsync(1, msg => 
                    Business(msg.Record).ContinueWith(done => (ICommittable) msg.CommitableOffset))
                .ToMaterialized(
                    Committer.Sink(committerDefaults.WithMaxBatch(1)), 
                    DrainingControl<NotUsed>.Create)
                .Run(materializer);
            
            // Uncomment for simple no-commit consumer
            /*
            await KafkaConsumer.PlainSource(consumerSettings, subscription)
                .RunForeach(result =>
                {
                    Console.WriteLine($"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Value}");
                }, materializer);
            */

            Console.WriteLine("Press any key to stop consumer.");
            Console.ReadKey();

            // Comment for simple no-commit consumer
            await control.Stop();
            await system.Terminate();
        }

        private static Task Business(ConsumeResult<Null, string> record)
        {
            Console.WriteLine($"Consumer: {record.Topic}/{record.Partition} {record.Offset}: {record.Message.Value}");
            return Task.CompletedTask;
        }
    }

    public class SinkActor : ReceiveActor
    {

    }
}
