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
                case AssignmentWithOffset awo:
                    consumer.Tell(new Internal.AssignWithOffset(awo.TopicPartitions), Self);
                    tps = tps.Union(awo.TopicPartitions.Keys);
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
                
            }
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
