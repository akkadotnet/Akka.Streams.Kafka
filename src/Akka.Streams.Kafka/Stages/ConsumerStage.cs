using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;
using Akka.Streams.Supervision;
using System.Runtime.Serialization;

namespace Akka.Streams.Kafka.Stages
{
    internal class KafkaSourceStage<K, V, Msg> : GraphStageWithMaterializedValue<SourceShape<Msg>, Task>
    {
        public Outlet<Msg> Out { get; } = new Outlet<Msg>("kafka.consumer.out");
        public override SourceShape<Msg> Shape { get; }
        public ConsumerSettings<K, V> Settings { get; }
        public ISubscription Subscription { get; }

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            Settings = settings;
            Subscription = subscription;
            Shape = new SourceShape<Msg>(Out);
            Settings = settings;
            Subscription = subscription;
        }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            return new LogicAndMaterializedValue<Task>(new KafkaSourceStageLogic<K, V, Msg>(this, inheritedAttributes, completion), completion.Task);
        }
    }

    internal class KafkaSourceStageLogic<K, V, Msg> : TimerGraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private Consumer<K, V> _consumer;

        private Action<Message<K, V>> _messagesReceived;
        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;
        private Action<IEnumerable<TopicPartition>> _partitionsRevoked;
        private readonly Decider _decider;

        private const string TimerKey = "PollTimer";

        private readonly Queue<Message<K, V>> _buffer = new Queue<Message<K, V>>();
        private readonly TaskCompletionSource<NotUsed> _completion;

        public KafkaSourceStageLogic(KafkaSourceStage<K, V, Msg> stage, Attributes attributes, TaskCompletionSource<NotUsed> completion) : base(stage.Shape)
        {
            _settings = stage.Settings;
            _subscription = stage.Subscription;
            _out = stage.Out;
            _completion = completion;

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.StoppingDecider;

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
            Log.Debug($"Consumer started: {_consumer.Name}");

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
        }

        public override void PostStop()
        {
            _consumer.OnMessage -= HandleOnMessage;
            _consumer.OnConsumeError -= HandleConsumeError;
            _consumer.OnError -= HandleOnError;
            _consumer.OnPartitionsAssigned -= HandleOnPartitionsAssigned;
            _consumer.OnPartitionsRevoked -= HandleOnPartitionsRevoked;

            Log.Debug($"Consumer stopped: {_consumer.Name}");
            _consumer.Dispose();

            base.PostStop();
        }

        //
        // Consumer's events
        //

        private void HandleOnMessage(object sender, Message<K, V> message) => _messagesReceived.Invoke(message);

        private void HandleConsumeError(object sender, Message message)
        {
            Log.Error(message.Error.Reason);
            var exception = new SerializationException(message.Error.Reason);
            switch (_decider(exception))
            {
                case Directive.Stop:
                    // Throw
                    _completion.TrySetException(exception);
                    FailStage(exception);
                    break;
                case Directive.Resume:
                    // TODO: resume the stream
                    break;
                case Directive.Restart:
                    // TODO: restart the stream
                    break;
            }
        }

        private void HandleOnError(object sender, Error error)
        {
            Log.Error(error.Reason);

            if (!KafkaExtensions.IsBrokerErrorRetriable(error) && !KafkaExtensions.IsLocalErrorRetriable(error))
            {
                var exception = new KafkaException(error);
                switch (_decider(exception))
                {
                    case Directive.Stop:
                        // Throw
                        _completion.TrySetException(exception);
                        FailStage(exception);
                        break;
                    case Directive.Resume:
                        // TODO: resume the stream
                        break;
                    case Directive.Restart:
                        // TODO: restart the stream
                        break;
                }
            }
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
            Log.Debug($"Partitions were assigned: {_consumer.Name}");
            _consumer.Assign(partitions);
        }

        private void PartitionsRevoked(IEnumerable<TopicPartition> partitions)
        {
            Log.Debug($"Partitions were revoked: {_consumer.Name}");
            _consumer.Unassign();
        }

        private void PullQueue()
        {
            // TODO: should I call `Poll` if there are no assignments? Like in `Subscribe` flow
            _consumer.Poll(_settings.PollTimeout);

            if (_buffer.Count == 0)
                ScheduleOnce(TimerKey, _settings.PollInterval);
        }

        protected override void OnTimer(object timerKey) => PullQueue();
    }
}
