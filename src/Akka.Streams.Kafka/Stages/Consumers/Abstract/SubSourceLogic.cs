using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Streams.Stage;
using Akka.Streams.Util;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// Stage logic used to produce sub-sources per topic partitions
    /// </summary>
    internal class SubSourceLogic<K, V, TMessage> : TimerGraphStageLogic
    {
        private class CloseRevokedPartitions { }

        private readonly SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> _shape;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly IAutoSubscription _subscription;
        private readonly IMessageBuilder<K, V, TMessage> _messageBuilder;
        private readonly Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> _getOffsetsOnAssign;
        private readonly Action<IImmutableSet<TopicPartition>> _onRevoke;
        private readonly TaskCompletionSource<NotUsed> _completion;

        private readonly int _actorNumber = KafkaConsumerActorMetadata.NextNumber();
        private Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
        private Action<IImmutableSet<TopicPartition>> _updatePendingPartitionsAndEmitSubSourcesCallback;
        private Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;
        private Action<(TopicPartition, Option<ConsumeResult<K, V>>)> _subsourceCancelledCallback;
        private Action<(TopicPartition, TaskCompletionSource<Done>)> _subsourceStartedCallback;
        private Action<ConsumerFailed> _stageFailCallback;
        
        /// <summary>
        /// Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet.
        /// </summary>
        private IImmutableSet<TopicPartition> _pendingPartitions = ImmutableHashSet<TopicPartition>.Empty;
        
        /// <summary>
        /// We have created a source for these partitions, but it has not started up and is not in subSources yet.
        /// </summary>
        private IImmutableSet<TopicPartition> _partitionsInStartup = ImmutableHashSet<TopicPartition>.Empty;
        private IImmutableDictionary<TopicPartition, TaskCompletionSource<Done>> _subSources = ImmutableDictionary<TopicPartition, TaskCompletionSource<Done>>.Empty;
        
        /// <summary>
        /// Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking.
        /// </summary>
        private IImmutableSet<TopicPartition> _partitionsToRevoke = ImmutableHashSet<TopicPartition>.Empty;
        

        protected StageActor SourceActor { get; private set; }
        protected IActorRef ConsumerActor { get; private set; }

        /// <summary>
        /// SubSourceLogic
        /// </summary>
        public SubSourceLogic(SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> shape, ConsumerSettings<K, V> settings,
                              IAutoSubscription subscription, Func<SubSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory,
                              Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> getOffsetsOnAssign,
                              Action<IImmutableSet<TopicPartition>> onRevoke, TaskCompletionSource<NotUsed> completion) 
            : base(shape)
        {
            _shape = shape;
            _settings = settings;
            _subscription = subscription;
            _messageBuilder = messageBuilderFactory(this);
            _getOffsetsOnAssign = getOffsetsOnAssign;
            _onRevoke = onRevoke;
            _completion = completion;

            _updatePendingPartitionsAndEmitSubSourcesCallback = GetAsyncCallback<IImmutableSet<TopicPartition>>(UpdatePendingPartitionsAndEmitSubSources);
            _partitionAssignedCallback = GetAsyncCallback<IImmutableSet<TopicPartition>>(HandlePartitionsAssigned);
            _partitionRevokedCallback = GetAsyncCallback<IImmutableSet<TopicPartitionOffset>>(HandlePartitionsRevoked);
            _stageFailCallback = GetAsyncCallback<ConsumerFailed>(FailStage);
            _subsourceCancelledCallback = GetAsyncCallback<(TopicPartition, Option<ConsumeResult<K, V>>)>(HandleSubsourceCancelled);
            _subsourceStartedCallback = GetAsyncCallback<(TopicPartition, TaskCompletionSource<Done>)>(HandleSubsourceStarted);
            
            SetHandler(shape.Outlet, onPull: EmitSubSourcesForPendingPartitions, onDownstreamFinish: PerformShutdown);
        }

        public override void PostStop()
        {
            ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Stop(), SourceActor.Ref);
            
            OnShutdown();
            
            base.PostStop();
        }

        protected override void OnTimer(object timerKey)
        {
            if (timerKey is CloseRevokedPartitions)
            {
                Log.Debug("#{} Closing SubSources for revoked partitions: {}", _actorNumber, _partitionsToRevoke.JoinToString(", "));

                _onRevoke(_partitionsToRevoke);
                _pendingPartitions = _pendingPartitions.Except(_partitionsToRevoke);
                _partitionsInStartup = _partitionsInStartup.Except(_partitionsToRevoke);
                _partitionsToRevoke.ForEach(tp => _subSources[tp].SetResult(Done.Instance));
                _subSources = _subSources.RemoveRange(_partitionsToRevoke);
                _partitionsToRevoke = _partitionsToRevoke.Clear();
            }
        }

        private async void HandlePartitionsAssigned(IImmutableSet<TopicPartition> assigned)
        {
            var formerlyUnknown = assigned.Except(_partitionsToRevoke);

            if (Log.IsDebugEnabled && formerlyUnknown.Any())
            {
                Log.Debug("#{} Assigning new partitions: {}", _actorNumber, formerlyUnknown.JoinToString(", "));
            }
            
            // make sure re-assigned partitions don't get closed on CloseRevokedPartitions timer
            _partitionsToRevoke = _partitionsToRevoke.Except(assigned);

            if (!_getOffsetsOnAssign.HasValue)
            {
                UpdatePendingPartitionsAndEmitSubSources(formerlyUnknown);
            }
            else
            {
                try
                {
                    var offsets = await _getOffsetsOnAssign.Value(assigned);
                    
                    SeekAndEmitSubSources(formerlyUnknown, offsets);
                }
                catch (Exception ex)
                {
                    _stageFailCallback(new ConsumerFailed($"{_actorNumber} Failed to fetch offset for partitions: {formerlyUnknown.JoinToString(", ")}", ex));
                }
            }
        }
        
        private async void SeekAndEmitSubSources(IImmutableSet<TopicPartition> formerlyUnknown, IImmutableSet<TopicPartitionOffset> offsets)
        {
            try
            {
                await ConsumerActor.Ask(new KafkaConsumerActorMetadata.Internal.Seek(offsets), TimeSpan.FromSeconds(10));
                
                UpdatePendingPartitionsAndEmitSubSources(formerlyUnknown);
            }
            catch (AskTimeoutException ex)
            {
                _stageFailCallback(new ConsumerFailed($"{_actorNumber} Consumer failed during seek for partitions: {offsets.JoinToString(", ")}"));
            }
        }

        private void HandlePartitionsRevoked(IImmutableSet<TopicPartitionOffset> revoked)
        {
            _partitionsToRevoke = _partitionsToRevoke.Union(revoked.Select(r => r.TopicPartition));
            
            ScheduleOnce(new CloseRevokedPartitions(), _settings.WaitClosePartition);
        }
        
        private void HandleSubsourceCancelled((TopicPartition, Option<ConsumeResult<K, V>>) obj)
        {
            var (topicPartition, firstUnconsumed) = obj;

            _subSources = _subSources.Remove(topicPartition);
            _partitionsInStartup = _partitionsInStartup.Remove(topicPartition);
            _pendingPartitions = _pendingPartitions.Add(topicPartition);

            if (firstUnconsumed.HasValue)
            {
                var topicPartitionOffset = new TopicPartitionOffset(topicPartition, firstUnconsumed.Value.Offset);
                Log.Debug("#{} Seeking {} to {} after partition SubSource cancelled", _actorNumber, topicPartition, topicPartitionOffset.Offset);
                
                SeekAndEmitSubSources(formerlyUnknown: ImmutableHashSet<TopicPartition>.Empty, offsets: ImmutableList.Create(topicPartitionOffset).ToImmutableHashSet());
            }
            else
            {
                EmitSubSourcesForPendingPartitions();
            }
        }
        
        private void HandleSubsourceStarted((TopicPartition, TaskCompletionSource<Done>) obj)
        {
            var (topicPartition, taskCompletionSource) = obj;

            if (!_partitionsInStartup.Contains(topicPartition))
            {
                // Partition was revoked while starting up. Kill!
                taskCompletionSource.SetResult(Done.Instance);
            }
            else
            {
                _subSources.SetItem(topicPartition, taskCompletionSource);
                _partitionsInStartup.Remove(topicPartition);
            }
        }

        private void UpdatePendingPartitionsAndEmitSubSources(IImmutableSet<TopicPartition> formerlyUnknownPartitions)
        {
            _pendingPartitions = _pendingPartitions.Union(formerlyUnknownPartitions.Where(tp => !_partitionsInStartup.Contains(tp)));
            
            EmitSubSourcesForPendingPartitions();
        }

        private void EmitSubSourcesForPendingPartitions()
        {
            if (_pendingPartitions.Any() && IsAvailable(_shape.Outlet))
            {
                var topicPartition = _pendingPartitions.First();

                _pendingPartitions = _pendingPartitions.Skip(1).ToImmutableHashSet();
                _partitionsInStartup = _partitionsInStartup.Add(topicPartition);
                
                var subSourceStage = new SubSourceStage<K, V, TMessage>(topicPartition, ConsumerActor, _subsourceStartedCallback, _subsourceCancelledCallback, _messageBuilder, _actorNumber);
                var subsource = Source.FromGraph(subSourceStage);
                
                Push(_shape.Outlet, (topicPartition, subsource));
                
                EmitSubSourcesForPendingPartitions();
            }
        }
        
        /// <summary>
        /// Makes this logic task finished
        /// </summary>
        protected void OnShutdown()
        {
            _completion.TrySetResult(NotUsed.Instance);
        }

        private void PerformShutdown()
        {
            SetKeepGoing(true);

            // TODO from alpakka: we should wait for subsources to be shutdown and next shutdown main stage
            _subSources.Values.ForEach(task => task.SetResult(Done.Instance));
            
            if (!IsClosed(_shape.Outlet))
                Complete(_shape.Outlet);
            
            SourceActor.Become(args =>
            {
                var (actor, message) = args;
                if (message is Terminated terminated && terminated.ActorRef.Equals(ConsumerActor))
                {
                    OnShutdown();
                    CompleteStage();
                }
            });
            
            Materializer.ScheduleOnce(_settings.StopTimeout, () => ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Stop()));
        }

        private class SubSourceStage<K, V, TMessage> : GraphStage<SourceShape<TMessage>>
        {
            public override SourceShape<TMessage> Shape { get; }

            public SubSourceStage(TopicPartition topicPartition, IActorRef consumerActor,
                                  Action<(TopicPartition, TaskCompletionSource<Done>)> subSourceStartedCallback,
                                  Action<(TopicPartition, Option<ConsumeResult<K, V>>)> subSourceCancelledCallback,
                                  IMessageBuilder<K, V, TMessage> messageBuilder,
                                  int actorNumber)
            {
                // TODO
            }
            
            
            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => throw new NotImplementedException();
        }
    }
}