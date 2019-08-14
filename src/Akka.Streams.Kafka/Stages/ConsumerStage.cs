using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;
using Akka.Streams.Supervision;
using System.Runtime.Serialization;
using System.Threading;

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

    internal class KafkaSourceStageLogic<K, V> : GraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private IConsumer<K, V> _consumer;

        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;
        private Action<IEnumerable<TopicPartitionOffset>> _partitionsRevoked;
        private readonly Decider _decider;

        private IEnumerable<TopicPartition> _assignedPartitions;
        private readonly TaskCompletionSource<NotUsed> _completion;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaSourceStageLogic(KafkaSourceStage<K, V> stage, Attributes attributes, TaskCompletionSource<NotUsed> completion) : base(stage.Shape)
        {
            _settings = stage.Settings;
            _subscription = stage.Subscription;
            _completion = completion;
            _cancellationTokenSource = new CancellationTokenSource();

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.ResumingDecider;

            SetHandler(stage.Out, onPull: () =>
            {
                try
                {
                    var message = _consumer.Consume(_cancellationTokenSource.Token);
                    if (message == null) // No message received, or consume error occured
                        return;

                    if (IsAvailable(stage.Out))
                    {
                        Push(stage.Out, message);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Consume was canceled, looks like we are shutting down the stage
                }
                catch (ConsumeException ex)
                {
                    HandleError(ex.Error);
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

            _partitionsAssigned = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsAssigned);
            _partitionsRevoked = GetAsyncCallback<IEnumerable<TopicPartitionOffset>>(PartitionsRevoked);
        }

        public override void PostStop()
        {
            Log.Debug($"Consumer stopped: {_consumer.Name}");
            _consumer.Dispose();

            base.PostStop();
        }

        //
        // Consumer's events
        //

        private void HandlePartitionsAssigned(IConsumer<K, V> consumer, List<TopicPartition> list)
        {
            _partitionsAssigned(list);
        }
        
        private void HandlePartitionsRevoked(IConsumer<K, V> consumer, List<TopicPartitionOffset> currentOffsets)
        {
            _partitionsRevoked(currentOffsets);
        }
        
        private void PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Log.Debug($"Partitions were assigned: {_consumer.Name}");
            var partitionsList = partitions.ToList();
            _consumer.Assign(partitionsList);
            _assignedPartitions = partitionsList;
        }
        
        private void PartitionsRevoked(IEnumerable<TopicPartitionOffset> partitions)
        {
            Log.Debug($"Partitions were revoked: {_consumer.Name}");
            _consumer.Unassign();
            _assignedPartitions = null;
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
                    _cancellationTokenSource.Cancel();
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
                        _cancellationTokenSource.Cancel();
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
