using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.EventSourcing.Log
{
    public class KafkaEventLog
    {
        public KafkaEventLog(string host, int port, ActorSystem system)
        {
            Host = host;
            Port = port;
        }

        public string Host { get; }
        public int Port { get; }

        public Flow<IEmitted<T>, IDelivery<IDurable<T>>, NotUsed> Flow<T>(TopicPartition topicPartition)
        {
            return null;
        }

        public Source<IDelivery<IDurable<T>>, NotUsed> Source<T>(TopicPartition topicPartition)
        {
            return null;
        }

        public Sink<IEmitted<T>, Task> Sink<T>(TopicPartition topicPartition)
        {
            //KafkaCodec.Encoder<T>(topicPartition).ToMaterialized(null, Keep.Right)
            return null;
        }
    }

    public class KafkaCodec
    {
        public static Flow<IDelivery<Message<string, IEmitted<object>>>, IDelivery<IDurable<A>>, NotUsed> Decoder<A>()
        {
            return Flow
                .Create<IDelivery<Message<string, IEmitted<object>>>>()
                .Collect(delivery =>
                {
                    if (delivery is Delivered<Message<string, IEmitted<object>>> d)
                    {
                        // TODO:
                        var durable = new Durable<A>(default(A), d.EventType.Value.EmitterId, d.EventType.Value.EmissionGuid, 6);
                        return new Delivered<IDurable<A>>(durable) as IDelivery<IDurable<A>>;
                    }

                    // TODO:
                    return default(Delivered<IDurable<A>>);
                });
        }

        public static Flow<IEmitted<A>, Kafka.Messages.ProduceRecord<string, IEmitted<A>>, NotUsed> Encoder<A>(TopicPartition topicPartition)
        {
            return Flow
                .Create<IEmitted<A>>()
                .Select(emitted => new Kafka.Messages.ProduceRecord<string, IEmitted<A>>(topicPartition.Topic,
                    emitted.EmitterId, emitted, topicPartition.Partition));
        }
    }

    public class KafkaMetadataSource<K, V, R> : GraphStage<SourceShape<R>>
    {
        public KafkaMetadataSource(ConsumerSettings<K, V> settings)
        {
            Settings = settings;
            Shape = new SourceShape<R>(Out);
        }

        public Outlet<R> Out { get; } = new Outlet<R>("KafkaMetadataSource.out");
        public override SourceShape<R> Shape { get; }
        public ConsumerSettings<K, V> Settings { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            throw new NotImplementedException();
        }

        internal class KafkaMetadataStageLogic<K, V, R> : GraphStageLogic
        {
            private readonly KafkaMetadataSource<K, V, R> _stage;
            private Consumer<K, V> _consumer;

            public KafkaMetadataStageLogic(KafkaMetadataSource<K, V, R> stage) : base(stage.Shape)
            {
                _stage = stage;

                SetHandler(_stage.Out, () =>
                {
                    // TODO: push something
                    Push(_stage.Out, "");
                });
            }

            public override void PreStart()
            {
                _consumer = _stage.Settings.CreateKafkaConsumer();
            }

            public override void PostStop()
            {
                _consumer.Dispose();
            }
        }
    }
}
