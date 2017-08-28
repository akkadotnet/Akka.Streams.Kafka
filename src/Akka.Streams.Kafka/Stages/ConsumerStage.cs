using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
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
            return new LogicAndMaterializedValue<Task>(new KafkaSourceStage<K, V>(_settings, _subscription, Shape), Task.CompletedTask);
        }
    }

    internal class KafkaSourceStage<K, V> : GraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private Consumer<K, V> consumer;

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, Shape shape) : base(shape)
        {
            _settings = settings;
            _subscription = subscription;
            _out = shape.Outlets.FirstOrDefault();

            SetHandler(_out,
                onPull: () =>
                {

                },
                onDownstreamFinish: () =>
                {

                });
        }

        public override void PreStart()
        {
            base.PreStart();

            var callback = GetAsyncCallback<Message<K, V>>(data =>
            {
                Push(_out, data);
            });

            consumer = _settings.CreateKafkaConsumer();
            consumer.OnMessage += ConsumerOnMessage;

            switch (_subscription)
            {
                case TopicSubscription ts:
                    consumer.Subscribe(ts.Topics);
                    break;
                case Assignment a:
                    consumer.Assign(a.TopicPartitions);
                    break;
                case AssignmentWithOffset awo:
                    consumer.Assign(awo.TopicPartitions);
                    break;
            }

            void ConsumerOnMessage(object sender, Message<K, V> message)
            {
                callback.Invoke(message);
            }

            InfinitePoll(_settings.PollTimeout);
        }

        public async Task InfinitePoll(TimeSpan timeout)
        {
            while (true)
            {
                consumer.Poll(timeout);
                await Task.Delay(_settings.PollInterval);
            }
        }

        public override void PostStop()
        {
            base.PostStop();

            consumer.Dispose();
        }
    }
}
