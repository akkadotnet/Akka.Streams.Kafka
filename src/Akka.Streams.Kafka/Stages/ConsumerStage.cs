using System;
using System.Collections.Generic;
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

    internal class KafkaSourceStage<K, V> : TimerGraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private Consumer<K, V> _consumer;
        private Action<Message<K, V>> _messagesReceived;
        private const string TimerKey = "PollTimer";

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, Shape shape) : base(shape)
        {
            _settings = settings;
            _subscription = subscription;
            _out = shape.Outlets.FirstOrDefault();

            SetHandler(_out, PullQueue);
        }

        public override void PreStart()
        {
            base.PreStart();

            _consumer = _settings.CreateKafkaConsumer();
            _consumer.OnMessage += OnMessage;
            _consumer.OnConsumeError += OnConsumeError;
            _consumer.OnError += OnError;

            switch (_subscription)
            {
                case TopicSubscription ts:
                    _consumer.Subscribe(ts.Topics);
                    break;
                case Assignment a:
                    _consumer.Assign(a.TopicPartitions);
                    break;
                case AssignmentWithOffset awo:
                    _consumer.Assign(awo.TopicPartitions);
                    break;
            }

            _messagesReceived = GetAsyncCallback<Message<K, V>>(OnMessagesReceived);

            ScheduleRepeatedly("poll", _settings.PollInterval);
        }
        
        private void OnMessage(object sender, Message<K, V> message)
        {
            _messagesReceived.Invoke(message);
        }

        private void OnConsumeError(object sender, Message message)
        {
            // On consume error
        }

        private void OnError(object sender, Error error)
        {
            Log.Error(error.Reason);
        }

        private void OnMessagesReceived(Message<K, V> message)
        {
            if (message.Error.HasError)
            {
                if (message.Error.Code == ErrorCode.BrokerNotAvailable)
                    FailStage(new Exception("Broker is not available"));
                else
                    ScheduleOnce(TimerKey, _settings.PollTimeout);
                return;
            }

            Push(_out, message);
            ScheduleOnce(TimerKey, _settings.PollTimeout);
        }

        public override void PostStop()
        {
            base.PostStop();

            _consumer.Dispose();
        }

        private void PullQueue() => _consumer.Poll(_settings.PollTimeout);

        protected override void OnTimer(object timerKey) => PullQueue();
    }
}
