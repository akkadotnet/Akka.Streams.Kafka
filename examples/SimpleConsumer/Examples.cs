using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Consumer = Akka.Streams.Kafka.Dsl.Consumer;

namespace SimpleConsumer
{
    public sealed class Db
    {
        private long offset;

        public Task<Done> Save(Message<Null, string> record)
        {
            Interlocked.Exchange(ref offset, record.Offset);
            return Task.FromResult(Done.Instance);
        }

        public Task<long> LoadOffset()
        {
            return Task.FromResult(offset);
        }

        public Task Update(string data)
        {
            return Task.CompletedTask;
        }
    }

    public static class Examples
    {
        public static void ExternalOffsetStorage()
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers("localhost:9092")
                .WithGroupId("group1");

            var db = new Db();
            var offset = db.LoadOffset().Result;
            var partition = 0;
            var subscription = Subscriptions.AssignmentWithOffset(new TopicPartitionOffset("topic1", partition, new Offset(offset)));
            Consumer.PlainSource(consumerSettings, subscription)
                .SelectAsync(1, db.Save)
                .RunWith(Sink.Ignore<Done>(), materializer);
        }

        public static void OffsetStorageInKafka()
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers("localhost:9092")
                .WithGroupId("group1");

            var db = new Db();
            var subscription = Subscriptions.Topics("akka");
            Consumer.CommittableSource(consumerSettings, subscription)
                .SelectAsync(1, msg => db.Update(msg.Record.Value).ContinueWith(task => msg))
                .SelectAsync(1, msg => msg.CommitableOffset.Commit())
                .RunWith(Sink.Ignore<CommittedOffsets>(), materializer);
        }

        public static void OffsetStorageBatch()
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers("localhost:9092")
                .WithGroupId("group1");

            var db = new Db();
            var subscription = Subscriptions.Topics("akka");
            Consumer.CommittableSource(consumerSettings, subscription)
                .SelectAsync(1, msg => db.Update(msg.Record.Value).ContinueWith(task => msg.CommitableOffset))
                .Batch(
                    max: 20, 
                    seed: CommittableOffsetBatch.Empty.Updated, 
                    aggregate: (batch, elem) => batch.Updated(elem))
                .SelectAsync(3, c => c.Commit())
                .RunWith(Sink.Ignore<CommittedOffsets>(), materializer);
        }
    }
}
