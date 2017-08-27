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

            var db = new Db();
            var offset = db.LoadOffset();
            var partition = 0;

            var subscription = Subscriptions.AssignmentWithOffset((new TopicPartition("topic1", partition), offset));

            Consumer.PlainSource(consumerSettings, subscription)
                .SelectAsync(1, db.Save)
                .RunWith(Sink.Ignore<Message<Null, string>>(), materializer);



            string brokerList = "localhost:9092";
            var topics = new List<string> { "akka" };

            var config = new Dictionary<string, object>
            {
                { "group.id", "simple-csharp-consumer" },
                { "bootstrap.servers", brokerList }
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                Console.WriteLine($"{consumer.Name} consuming on {topics[0]}. q to exit.");
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topics.First(), 0, 0) });

                while (true)
                {
                    if (consumer.Consume(out Message<Null, string> msg, TimeSpan.FromSeconds(1)))
                    {
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    }
                }
            }
        }
    }

    public class Db
    {
        private long _offset = 0L;

        public Task<Message<Null, string>> Save(Message<Null, string> record)
        {
            Console.WriteLine($"DB.save: {record.Value}");
            Interlocked.Add(ref _offset, record.Offset.Value);
            return Task.FromResult(record);
        }

        public long LoadOffset()
        {
            return _offset;
        }

        public Task Update(string data)
        {
            Console.WriteLine($"DB.update: {data}");
            return Task.CompletedTask;
        }
    }
}
