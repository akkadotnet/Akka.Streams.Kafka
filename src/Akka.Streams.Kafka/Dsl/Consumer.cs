using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Dsl
{
    public interface IControl
    {
        Task Stop();
        Task Shutdown();
        Task IsShutdown();
    }

    public static class Consumer
    {
        public static Source<Message<K, V>, Task> PlainSource<K, V>(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            return Source.FromGraph(new KafkaSimpleSourceStage<K, V, Message<K, V>>(settings, subscription));
        }
    }
}
