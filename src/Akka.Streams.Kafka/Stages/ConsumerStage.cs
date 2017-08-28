using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    internal class KafkaSourceStage<K, V, Msg> : GraphStageWithMaterializedValue<SourceShape<Msg>, Task>
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;

        protected readonly Outlet<Msg> Out = new Outlet<Msg>("out");
        public override SourceShape<Msg> Shape { get; }

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            _settings = settings;
            _subscription = subscription;
            Shape = new SourceShape<Msg>(Out);
        }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            return new LogicAndMaterializedValue<Task>(new SingleSourceLogic<K, V>(_settings, _subscription, Shape), Task.CompletedTask);
        }
    }

    internal class SingleSourceLogic<K, V> : GraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private IActorRef consumer;
        private StageActorRef Self;
        private IImmutableSet<TopicPartition> tps = ImmutableHashSet<TopicPartition>.Empty;

        
        private int requestId = 0;
        private bool requested = false;

        private Queue<Message<K, V>> buffer = new Queue<Message<K, V>>();

        public SingleSourceLogic(ConsumerSettings<K, V> settings, ISubscription subscription, Shape shape) : base(shape)
        {
            _settings = settings;
            _subscription = subscription;
            _out = shape.Outlets.FirstOrDefault();

            SetHandler(_out, 
                onPull: () =>
                {
                    Pump();
                },
                onDownstreamFinish: () =>
                {
                    PerformShutdown();
                });
        }

        public override void PreStart()
        {
            base.PreStart();

            var extendedActorSystem = ActorMaterializerHelper.Downcast(Materializer).System.AsInstanceOf<ExtendedActorSystem>();
            var name = $"kafka-consumer-{KafkaConsumerActor.NextNumber()}";
            consumer = extendedActorSystem.SystemActorOf(KafkaConsumerActor.Props(_settings), name);

            Self = GetStageActorRef(args =>
            {
                switch (args.Item2)
                {
                    case Internal.Messages<K, V> msg:
                        // might be more than one in flight when we assign/revoke tps
                        if (msg.RequestId == requestId)
                            requested = true;
                        if (buffer.Peek() != null)
                        {
                            foreach (var message in msg.KafkaMessages)
                            {
                                buffer.Enqueue(message);
                            }
                        }
                        else
                        {
                            buffer = new Queue<Message<K, V>>(msg.KafkaMessages);
                        }
                        Pump();
                        break;
                    case Terminated t when t.ActorRef.Equals(consumer):
                        FailStage(new Exception("Consumer actor terminated"));
                        break;
                }
            });
            Self.Watch(consumer);

            object rebalanceListener = null;

            switch (_subscription)
            {
                case TopicSubscription ts:
                    consumer.Tell(new Internal.Subscribe(ts.Topics, rebalanceListener), Self);
                    break;
                case TopicSubscriptionPattern tsp:
                    // TODO: implement
                    break;
                case Assignment a:
                    consumer.Tell(new Internal.Assign(a.TopicPartitions), Self);
                    tps = tps.Union(a.TopicPartitions);
                    break;
            }
        }

        public override void PostStop()
        {
            base.PostStop();
        }

        private void Pump()
        {
            if (IsAvailable(_out))
            {
                if (buffer.Count > 0)
                {
                    var msg = buffer.Dequeue();
                    Push(_out, msg);
                    Pump();
                }
                else if (!requested && tps.Count > 0)
                {
                    RequestMessages();
                }
            }
        }

        public void RequestMessages()
        {
            requested = true;
            requestId += 1;
            consumer.Tell(new Internal.RequestMessages(requestId, tps), Self);
        }

        private void PerformShutdown()
        {
            SetKeepGoing(true);
            if (!IsClosed(_out))
            {
                Complete(_out);
            }
        }
    }
}
