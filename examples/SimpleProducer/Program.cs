using System;
using System.Linq;
using System.Text;
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

namespace SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var producerSettings = ProducerSettings<string, string>.Create(system, null, null)
                .WithBootstrapServers("localhost:29092");
            
            Source
                .Cycle(() => Enumerable.Range(1, 1000).GetEnumerator())
                .Throttle(1, TimeSpan.FromMilliseconds(200), 1, ThrottleMode.Shaping)
                .Select(c => c.ToString())
                .Select(elem => ProducerMessage.Single(new ProducerRecord<string, string>("akka100", $"key-{elem}", elem)))
                .Via(KafkaProducer.FlexiFlow<string, string, NotUsed>(producerSettings))
                .Select(result =>
                {
                    var response = (Result<string, string, NotUsed>)result;
                    var meta = response.Metadata;
                    Console.WriteLine($"Producer: {meta.Topic}/{meta.Partition} {meta.Offset}: {meta.Value}");
                    return result;
                })
                .RunWith(Sink.Ignore<IResults<string, string, NotUsed>>(), materializer);

            // TODO: producer as a Commitable Sink

            // TODO: Sharing KafkaProducer

            Console.ReadLine();
        }
    }
}
