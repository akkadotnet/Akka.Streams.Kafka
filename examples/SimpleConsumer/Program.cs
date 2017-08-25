using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace SimpleConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
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
}
