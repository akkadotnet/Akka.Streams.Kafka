using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    internal class CommitableConsumerStage<K, V, Msg> : GraphStageWithMaterializedValue<SourceShape<Msg>, Task>
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;

        protected readonly Outlet<Msg> Out = new Outlet<Msg>("out");
        public override SourceShape<Msg> Shape { get; }

        public CommitableConsumerStage(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            _settings = settings;
            _subscription = subscription;
            Shape = new SourceShape<Msg>(Out);
        }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            return new LogicAndMaterializedValue<Task>(new KafkaCommitableSourceStage<K, V>(_settings, _subscription, Shape), Task.CompletedTask);
        }
    }

    internal class KafkaCommitableSourceStage<K, V> : TimerGraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private Consumer<K, V> _consumer;
        private Action<Message<K, V>> _messagesReceived;
        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;

        private const string TimerKey = "PollTimer";

        private readonly Queue<CommittableMessage<K, V>> _buffer = new Queue<CommittableMessage<K, V>>();

        public KafkaCommitableSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, Shape shape) : base(shape)
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
            _consumer.OnMessage += (sender, message) => _messagesReceived.Invoke(message);
            _consumer.OnConsumeError += OnConsumeError;
            _consumer.OnError += OnError;
            _consumer.OnPartitionsAssigned += (sender, list) => _partitionsAssigned.Invoke(list);

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
            _partitionsAssigned = GetAsyncCallback<IEnumerable<TopicPartition>>(OnPartitionsAssigned);
            ScheduleOnce(TimerKey, _settings.PollInterval);
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
            var consumer = _consumer;
            var commitableOffset = new CommitableOffset(() =>
            {
                consumer.CommitAsync();
            }, new PartitionOffset("groupId", message.Topic, message.Partition, message.Offset));

            _buffer.Enqueue(new CommittableMessage<K, V>(message, commitableOffset));
            if (IsAvailable(_out))
            {
                Push(_out, _buffer.Dequeue());
            }
        }

        private void OnPartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Log.Info("Partitions were assigned");
            _consumer.Assign(partitions);
            // TODO: should I call `PullQueue` here?
        }

        public override void PostStop()
        {
            base.PostStop();
            _consumer.Dispose();
        }

        private void PullQueue()
        {
            // TODO: should I call `Poll` if there are no assignments? Like in `Subscribe` flow
            _consumer.Poll(_settings.PollTimeout);
            ScheduleOnce(TimerKey, _settings.PollInterval);
        }

        protected override void OnTimer(object timerKey) => PullQueue();
    }
}
