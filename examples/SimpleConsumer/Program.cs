using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
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

            var partition = 0;

            //Consumer.PlainSource(consumerSettings, subscription)
            //    .SelectAsync(1, Task.FromResult)
            //    .Select(c =>
            //    {
            //        Console.WriteLine(c.Value);
            //        return c;
            //    })
            //    .RunWith(Sink.Ignore<Message<Null, string>>(), materializer);


            var consumer = consumerSettings.CreateKafkaConsumer();
            consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset("akka", 0, 0) });

            consumer.OnMessage += (sender, message) =>
            {

            };

            while (true)
            {
                consumer.Poll(consumerSettings.PollTimeout);
            }

            

            Console.ReadLine();
        }
    }
}
