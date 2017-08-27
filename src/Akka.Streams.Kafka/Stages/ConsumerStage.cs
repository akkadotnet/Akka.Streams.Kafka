using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;

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

            Self.Watch(consumer);

            switch (_subscription)
            {
                case TopicSubscription ts:
                    break;
                case TopicSubscriptionPattern tsp:
                    break;
                case Assignment a:
                    break;
                case AssignmentWithOffset awo:
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
