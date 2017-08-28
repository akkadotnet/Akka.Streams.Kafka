using System;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Consumer = Akka.Streams.Kafka.Dsl.Consumer;

namespace SimpleConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var fallbackConfig = ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf");

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers("localhost:9092")
                .WithGroupId("group1");

            var subscription = Subscriptions.Assignment(new TopicPartition("akka10", 0));
            //var subscription = Subscriptions.AssignmentWithOffset(new TopicPartitionOffset("akka", 0, new Offset(20)));
            //var subscription = Subscriptions.Topics("akka");

            Consumer.PlainSource(consumerSettings, subscription)
                .Select(result =>
                {
                    Console.WriteLine($"{result.Topic}/{result.Partition} {result.Offset}: {result.Value}");
                    return result;
                })
                .RunWith(Sink.Ignore<Message<Null, string>>(), materializer);

            Console.ReadLine();
        }
    }
}
