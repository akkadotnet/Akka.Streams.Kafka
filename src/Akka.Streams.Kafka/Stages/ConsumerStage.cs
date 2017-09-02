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
        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;
        private Action<IEnumerable<TopicPartition>> _partitionsRevoked;
        private Action _pullQueue;

        private const string TimerKey = "PollTimer";

        private readonly Queue<Message<K, V>> _buffer = new Queue<Message<K, V>>();

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, Shape shape) : base(shape)
        {
            _settings = settings;
            _subscription = subscription;
            _out = shape.Outlets.FirstOrDefault();

            SetHandler(_out, onPull:() =>
            {
                if (_buffer.Count > 0)
                {
                    Push(_out, _buffer.Dequeue());
                }
                else
                {
                    PullQueue();
                }
            });
        }

        public override void PreStart()
        {
            base.PreStart();

            _consumer = _settings.CreateKafkaConsumer();
            _consumer.OnMessage += HandleOnMessage;
            _consumer.OnConsumeError += HandleConsumeError;
            _consumer.OnError += HandleOnError;
            _consumer.OnPartitionsAssigned += HandleOnPartitionsAssigned;
            _consumer.OnPartitionsRevoked += HandleOnPartitionsRevoked;

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

            _messagesReceived = GetAsyncCallback<Message<K, V>>(MessagesReceived);
            _partitionsAssigned = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsAssigned);
            _partitionsRevoked = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsRevoked);
            _pullQueue = GetAsyncCallback(PullQueue);

            ScheduleOnce(TimerKey, _settings.PollInterval);
        }

        public override void PostStop()
        {
            _consumer.OnMessage -= HandleOnMessage;
            _consumer.OnConsumeError -= HandleConsumeError;
            _consumer.OnError -= HandleOnError;
            _consumer.OnPartitionsAssigned -= HandleOnPartitionsAssigned;
            _consumer.OnPartitionsRevoked -= HandleOnPartitionsRevoked;

            _consumer.Dispose();

            base.PostStop();
        }

        //
        // Consumer's events
        //

        private void HandleOnMessage(object sender, Message<K, V> message) => _messagesReceived.Invoke(message);

        // TODO: how I should react?
        private void HandleConsumeError(object sender, Message message) { }

        private void HandleOnError(object sender, Error error)
        {
            if (error.Code == ErrorCode.Local_Transport)
            {
                FailStage(new Exception(error.Reason));
            }

            // TODO: what else errors to handle?
            Log.Error(error.Reason);
        }

        private void HandleOnPartitionsAssigned(object sender, List<TopicPartition> list)
        {
            _partitionsAssigned.Invoke(list);
        }

        private void HandleOnPartitionsRevoked(object sender, List<TopicPartition> list)
        {
            _partitionsRevoked.Invoke(list);
        }

        //
        // Async callbacks
        //

        private void MessagesReceived(Message<K, V> message)
        {
            _buffer.Enqueue(message);
            if (IsAvailable(_out))
            {
                Push(_out, _buffer.Dequeue());
            }
        }

        private void PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Log.Info("Partitions were assigned");
            _consumer.Assign(partitions);
        }

        private void PartitionsRevoked(IEnumerable<TopicPartition> partitions)
        {
            Log.Info("Partitions were revoked");
            _consumer.Unassign();
        }

        private void PullQueue()
        {
            // TODO: should I call `Poll` if there are no assignments? Like in `Subscribe` flow
            _consumer.Poll(_settings.PollTimeout);
            ScheduleOnce(TimerKey, _settings.PollInterval);
        }

        protected override void OnTimer(object timerKey) => _pullQueue.Invoke();
    }
}
