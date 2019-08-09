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
    internal class KafkaSourceStage<K, V> : GraphStageWithMaterializedValue<SourceShape<ConsumeResult<K, V>>, Task>
    {
        public Outlet<ConsumeResult<K, V>> Out { get; } = new Outlet<ConsumeResult<K, V>>("kafka.consumer.out");
        public override SourceShape<ConsumeResult<K, V>> Shape { get; }
        public ConsumerSettings<K, V> Settings { get; }
        public ISubscription Subscription { get; }

        public KafkaSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            Settings = settings;
            Subscription = subscription;
            Shape = new SourceShape<ConsumeResult<K, V>>(Out);
            Settings = settings;
            Subscription = subscription;
        }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            return new LogicAndMaterializedValue<Task>(new KafkaSourceStageLogic<K, V>(this, inheritedAttributes, completion), completion.Task);
        }
    }

    internal class KafkaSourceStageLogic<K, V> : TimerGraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet<ConsumeResult<K, V>> _out;
        private IConsumer<K, V> _consumer;

        private Action<ConsumeResult<K, V>> _messagesReceived;
        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;
        private Action<IEnumerable<TopicPartitionOffset>> _partitionsRevoked;
        private readonly Decider _decider;

        private const string TimerKey = "PollTimer";

        private readonly Queue<ConsumeResult<K, V>> _buffer;
        private IEnumerable<TopicPartition> _assignedPartitions;
        private volatile bool _isPaused;
        private readonly TaskCompletionSource<NotUsed> _completion;

        public KafkaSourceStageLogic(KafkaSourceStage<K, V> stage, Attributes attributes, TaskCompletionSource<NotUsed> completion) : base(stage.Shape)
        {
            _settings = stage.Settings;
            _subscription = stage.Subscription;
            _out = stage.Out;
            _completion = completion;
            _buffer = new Queue<ConsumeResult<K, V>>(stage.Settings.BufferSize);

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.ResumingDecider;

            SetHandler(_out, onPull: () =>
            {
                if (_buffer.Count > 0)
                {
                    Push(_out, _buffer.Dequeue());
                }
                else
                {
                    if (_isPaused)
                    {
                        _consumer.Resume(_assignedPartitions);
                        _isPaused = false;
                        Log.Debug("Polling resumed, buffer is empty");
                    }
                    PullQueue();
                }
            });
        }

        public override void PreStart()
        {
            base.PreStart();

            _consumer = _settings.CreateKafkaConsumer(HandleConsumeError, HandlePartitionsAssigned, HandlePartitionsRevoked);
            Log.Debug($"Consumer started: {_consumer.Name}");

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

            _messagesReceived = GetAsyncCallback<ConsumeResult<K, V>>(MessagesReceived);
            _partitionsAssigned = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsAssigned);
            _partitionsRevoked = GetAsyncCallback<IEnumerable<TopicPartitionOffset>>(PartitionsRevoked);
            ScheduleRepeatedly(TimerKey, _settings.PollInterval);
        }

        public override void PostStop()
        {
            Log.Debug($"Consumer stopped: {_consumer.Name}");
            _consumer.Dispose();

            base.PostStop();
        }
        
        protected override void OnTimer(object timerKey) => PullQueue();

        //
        // Consumer's events
        //

        private void HandleMessage(ConsumeResult<K, V> message) => _messagesReceived(message);
        
        private void MessagesReceived(ConsumeResult<K, V> message)
        {
            _buffer.Enqueue(message);
            if (IsAvailable(_out))
            {
                Push(_out, _buffer.Dequeue());
            }
        }

        private void HandlePartitionsAssigned(IConsumer<K, V> consumer, List<TopicPartition> list)
        {
            _partitionsAssigned(list);
        }
        
        private void PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Log.Debug($"Partitions were assigned: {_consumer.Name}");
            _consumer.Assign(partitions);
            _assignedPartitions = partitions;
        }
        
        private void HandleConsumeError(IConsumer<K, V> consumer, Error error)
        {
            Log.Error(error.Reason);
            // var exception = new SerializationException(error.Reason);
            var exception = new KafkaException(error);
            switch (_decider(exception))
            {
                case Directive.Stop:
                    // Throw
                    _completion.TrySetException(exception);
                    FailStage(exception);
                    break;
                case Directive.Resume:
                    // keep going
                    break;
                case Directive.Restart:
                    // keep going
                    break;
            }
        }

        private void HandlePartitionsRevoked(IConsumer<K, V> consumer, List<TopicPartitionOffset> currentOffsets)
        {
            _partitionsRevoked(currentOffsets);
        }

        private void PartitionsRevoked(IEnumerable<TopicPartitionOffset> partitions)
        {
            Log.Debug($"Partitions were revoked: {_consumer.Name}");
            _consumer.Unassign();
            _assignedPartitions = null;
        }

        private void PullQueue()
        {
            try
            {
                var message = _consumer.Consume(_settings.PollTimeout);
                if (message == null) // Sone error occured, nothing to do here
                    return;
                
                HandleMessage(message);
            }
            catch (ConsumeException ex)
            {
                HandleError(ex.Error);
            }

            if (!_isPaused && _buffer.Count > _settings.BufferSize)
            {
                Log.Debug($"Polling paused, buffer is full");
                _consumer.Pause(_assignedPartitions);
                _isPaused = true;
            }
        }
        
        private void HandleError(Error error)
        {
            Log.Error(error.Reason);

            if (!KafkaExtensions.IsBrokerErrorRetriable(error) && !KafkaExtensions.IsLocalErrorRetriable(error))
            {
                var exception = new KafkaException(error);
                FailStage(exception);
            }
            else if (KafkaExtensions.IsLocalValueSerializationError(error))
            {
                var exception = new SerializationException(error.Reason);
                switch (_decider(exception))
                {
                    case Directive.Stop:
                        // Throw
                        _completion.TrySetException(exception);
                        FailStage(exception);
                        break;
                    case Directive.Resume:
                        // keep going
                        break;
                    case Directive.Restart:
                        // keep going
                        break;
                }
            }
        }
    }
}
