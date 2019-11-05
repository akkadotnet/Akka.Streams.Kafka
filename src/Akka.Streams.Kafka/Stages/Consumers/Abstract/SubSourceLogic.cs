using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Akka.Streams.Util;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

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

        private readonly int _actorNumber = KafkaConsumerActorMetadata.NextNumber();
        private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
        private Action<IImmutableSet<TopicPartition>> _updatePendingPartitionsAndEmitSubSourcesCallback;
        private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;
        private readonly Action<(TopicPartition, Option<ConsumeResult<K, V>>)> _subsourceCancelledCallback;
        private readonly Action<(TopicPartition, IControl)> _subsourceStartedCallback;
        private readonly Action<ConsumerFailed> _stageFailCallback;
        private readonly Decider _decider;
        
        /// <summary>
        /// Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet.
        /// </summary>
        private IImmutableSet<TopicPartition> _pendingPartitions = ImmutableHashSet<TopicPartition>.Empty;
        
        /// <summary>
        /// We have created a source for these partitions, but it has not started up and is not in subSources yet.
        /// </summary>
        private IImmutableSet<TopicPartition> _partitionsInStartup = ImmutableHashSet<TopicPartition>.Empty;
        private IImmutableDictionary<TopicPartition, IControl> _subSources = ImmutableDictionary<TopicPartition, IControl>.Empty;
        
        /// <summary>
        /// Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking.
        /// </summary>
        private IImmutableSet<TopicPartition> _partitionsToRevoke = ImmutableHashSet<TopicPartition>.Empty;
        

        protected StageActor SourceActor { get; private set; }
        public IActorRef ConsumerActor { get; private set; }
        
        public PromiseControl<(TopicPartition, Source<TMessage, NotUsed>)> Control { get; }

        /// <summary>
        /// SubSourceLogic
        /// </summary>
        public SubSourceLogic(SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> shape, ConsumerSettings<K, V> settings,
                              IAutoSubscription subscription, Func<SubSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory,
                              Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> getOffsetsOnAssign,
                              Action<IImmutableSet<TopicPartition>> onRevoke, Attributes attributes) 
            : base(shape)
        {
            _shape = shape;
            _settings = settings;
            _subscription = subscription;
            _messageBuilder = messageBuilderFactory(this);
            _getOffsetsOnAssign = getOffsetsOnAssign;
            _onRevoke = onRevoke;
            
            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.StoppingDecider;
            
            Control  = new SubSourcePromiseControl(_shape, Complete, SetKeepGoing, GetAsyncCallback, PerformStop, PerformShutdown);

            _updatePendingPartitionsAndEmitSubSourcesCallback = GetAsyncCallback<IImmutableSet<TopicPartition>>(UpdatePendingPartitionsAndEmitSubSources);
            _partitionAssignedCallback = GetAsyncCallback<IImmutableSet<TopicPartition>>(HandlePartitionsAssigned);
            _partitionRevokedCallback = GetAsyncCallback<IImmutableSet<TopicPartitionOffset>>(HandlePartitionsRevoked);
            _stageFailCallback = GetAsyncCallback<ConsumerFailed>(FailStage);
            _subsourceCancelledCallback = GetAsyncCallback<(TopicPartition, Option<ConsumeResult<K, V>>)>(HandleSubsourceCancelled);
            _subsourceStartedCallback = GetAsyncCallback<(TopicPartition, IControl)>(HandleSubsourceStarted);
            
            SetHandler(shape.Outlet, onPull: EmitSubSourcesForPendingPartitions, onDownstreamFinish: PerformShutdown);
        }

        public override void PreStart()
        {
            base.PreStart();

            SourceActor = GetStageActor(args =>
            {
                switch (args.Item2)
                {
                    case Status.Failure failure:
                        var exception = failure.Cause;
                        switch (_decider(failure.Cause))
                        {
                            case Directive.Stop:
                                // Throw
                                FailStage(exception);
                                break;
                            case Directive.Resume:
                                // keep going
                                break;
                            case Directive.Restart:
                                // TODO: Need to do something here: https://github.com/akkadotnet/Akka.Streams.Kafka/issues/33
                                break;
                        }
                        break;
                        
                    case Terminated terminated when terminated.ActorRef.Equals(ConsumerActor):
                        FailStage(new ConsumerFailed());
                        break;
                }
            });
            
            if (!(Materializer is ActorMaterializer actorMaterializer))
                throw new ArgumentException($"Expected {typeof(ActorMaterializer)} but got {Materializer.GetType()}");
            
            var eventHandler = new AsyncCallbacksPartitionEventHandler<K, V>(_partitionAssignedCallback, _partitionRevokedCallback);
            
            var extendedActorSystem = actorMaterializer.System.AsInstanceOf<ExtendedActorSystem>();
            ConsumerActor = extendedActorSystem.SystemActorOf(KafkaConsumerActorMetadata.GetProps(SourceActor.Ref, _settings, eventHandler), 
                                                              $"kafka-consumer-{_actorNumber}");
            
            SourceActor.Watch(ConsumerActor);

            switch (_subscription)
            {
                case TopicSubscription topicSubscription:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Subscribe(topicSubscription.Topics), SourceActor.Ref);
                    break;
                
                case TopicSubscriptionPattern topicSubscriptionPattern:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.SubscribePattern(topicSubscriptionPattern.TopicPattern), SourceActor.Ref);
                    break;
            }
        }

        public override void PostStop()
        {
            ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Stop(), SourceActor.Ref);
            
            Control.OnShutdown();
            
            base.PostStop();
        }

        protected override void OnTimer(object timerKey)
        {
            if (timerKey is CloseRevokedPartitions)
            {
                Log.Debug("#{0} Closing SubSources for revoked partitions: {1}", _actorNumber, _partitionsToRevoke.JoinToString(", "));

                _onRevoke(_partitionsToRevoke);
                _pendingPartitions = _pendingPartitions.Except(_partitionsToRevoke);
                _partitionsInStartup = _partitionsInStartup.Except(_partitionsToRevoke);
                _partitionsToRevoke.ForEach(tp => _subSources[tp].Shutdown());
                _subSources = _subSources.RemoveRange(_partitionsToRevoke);
                _partitionsToRevoke = _partitionsToRevoke.Clear();
            }
        }

        private async void HandlePartitionsAssigned(IImmutableSet<TopicPartition> assigned)
        {
            var formerlyUnknown = assigned.Except(_partitionsToRevoke);

            if (Log.IsDebugEnabled && formerlyUnknown.Any())
            {
                Log.Debug("#{0} Assigning new partitions: {1}", _actorNumber, formerlyUnknown.JoinToString(", "));
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
                // Without this delay kafka does not allow to perform seek operation (gives Local: Erroneous state error)
                await Task.Delay(50);
                
                await ConsumerActor.Ask(new KafkaConsumerActorMetadata.Internal.Seek(offsets), TimeSpan.FromSeconds(10));
                
                _updatePendingPartitionsAndEmitSubSourcesCallback(formerlyUnknown);
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
                Log.Debug("#{0} Seeking {1} to {2} after partition SubSource cancelled", _actorNumber, topicPartition, topicPartitionOffset.Offset);
                
                SeekAndEmitSubSources(formerlyUnknown: ImmutableHashSet<TopicPartition>.Empty, offsets: ImmutableList.Create(topicPartitionOffset).ToImmutableHashSet());
            }
            else
            {
                EmitSubSourcesForPendingPartitions();
            }
        }
        
        private void HandleSubsourceStarted((TopicPartition, IControl) obj)
        {
            var (topicPartition, control) = obj;

            if (!_partitionsInStartup.Contains(topicPartition))
            {
                // Partition was revoked while starting up. Kill!
                control.Shutdown();
            }
            else
            {
                _subSources = _subSources.SetItem(topicPartition, control);
                _partitionsInStartup = _partitionsInStartup.Remove(topicPartition);
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

                _pendingPartitions = _pendingPartitions.Remove(topicPartition);
                _partitionsInStartup = _partitionsInStartup.Add(topicPartition);
                
                var subSourceStage = new SubSourceStreamStage<K, V, TMessage>(topicPartition, ConsumerActor, _subsourceStartedCallback, 
                                                                              _subsourceCancelledCallback, _messageBuilder, _decider, _actorNumber);
                var subsource = Source.FromGraph(subSourceStage);
                
                Push(_shape.Outlet, (topicPartition, subsource));
                
                EmitSubSourcesForPendingPartitions();
            }
        }

        private void PerformStop()
        {
            SetKeepGoing(true);
            
            _subSources.Values.ForEach(control => control.Stop());
            
            Complete(_shape.Outlet);
            
            Control.OnStop();
        }

        private void PerformShutdown()
        {
            SetKeepGoing(true);

            // TODO from alpakka: we should wait for subsources to be shutdown and next shutdown main stage
            _subSources.Values.ForEach(control => control.Shutdown());
            
            if (!IsClosed(_shape.Outlet))
                Complete(_shape.Outlet);
            
            SourceActor.Become(args =>
            {
                var (actor, message) = args;
                if (message is Terminated terminated && terminated.ActorRef.Equals(ConsumerActor))
                {
                    Control.OnShutdown();
                    CompleteStage();
                }
            });
            
            Materializer.ScheduleOnce(_settings.StopTimeout, () => ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Stop()));
        }

        /// <summary>
        /// Overrides some method of base <see cref="PromiseControl{TSourceOut}"/>
        /// </summary>
        protected class SubSourcePromiseControl : PromiseControl<(TopicPartition, Source<TMessage, NotUsed>)>
        {
            private readonly Action _performStop;
            private readonly Action _performShutdown;

            public SubSourcePromiseControl(SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> shape, 
                                           Action<Outlet<(TopicPartition, Source<TMessage, NotUsed>)>> completeStageOutlet, 
                                           Action<bool> setStageKeepGoing, Func<Action, Action> asyncCallbackFactory,
                                           Action performStop, Action performShutdown) 
                : base(shape, completeStageOutlet, setStageKeepGoing, asyncCallbackFactory)
            {
                _performStop = performStop;
                _performShutdown = performShutdown;
            }

            /// <inheritdoc />
            public override void PerformStop() => _performStop();

            /// <inheritdoc />
            public override void PerformShutdown() => _performShutdown();
        }

        private class SubSourceStreamStage<K, V, TMsg> : GraphStage<SourceShape<TMsg>>
        {
            private readonly TopicPartition _topicPartition;
            private readonly IActorRef _consumerActor;
            private readonly Action<(TopicPartition, IControl)> _subSourceStartedCallback;
            private readonly Action<(TopicPartition, Option<ConsumeResult<K, V>>)> _subSourceCancelledCallback;
            private readonly IMessageBuilder<K, V, TMsg> _messageBuilder;
            private readonly int _actorNumber;
            private readonly Decider _decider;
            
            public Outlet<TMsg> Out { get; }
            public override SourceShape<TMsg> Shape { get; }

            public SubSourceStreamStage(TopicPartition topicPartition, IActorRef consumerActor,
                                  Action<(TopicPartition, IControl)> subSourceStartedCallback,
                                  Action<(TopicPartition, Option<ConsumeResult<K, V>>)> subSourceCancelledCallback,
                                  IMessageBuilder<K, V, TMsg> messageBuilder,
                                  Decider decider,
                                  int actorNumber)
            {
                _topicPartition = topicPartition;
                _consumerActor = consumerActor;
                _subSourceStartedCallback = subSourceStartedCallback;
                _subSourceCancelledCallback = subSourceCancelledCallback;
                _messageBuilder = messageBuilder;
                _decider = decider;
                _actorNumber = actorNumber;
                
                Out = new Outlet<TMsg>("out");
                Shape = new SourceShape<TMsg>(Out);
            }
            
            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
            {
                return new SubSourceStreamStageLogic(Shape, _topicPartition, _consumerActor, _actorNumber, _messageBuilder, _decider,
                                               _subSourceStartedCallback, _subSourceCancelledCallback);
            }
            
            private class SubSourceStreamStageLogic : GraphStageLogic
            {
                private readonly SourceShape<TMsg> _shape;
                private readonly TopicPartition _topicPartition;
                private readonly IActorRef _consumerActor;
                private readonly int _actorNumber;
                private readonly IMessageBuilder<K, V, TMsg> _messageBuilder;
                private readonly Action<(TopicPartition, IControl)> _subSourceStartedCallback;
                private readonly KafkaConsumerActorMetadata.Internal.RequestMessages _requestMessages;
                private bool _requested = false;
                private StageActor _subSourceActor;
                private readonly Decider _decider;
                private readonly ConcurrentQueue<ConsumeResult<K, V>> _buffer = new ConcurrentQueue<ConsumeResult<K, V>>();

                public PromiseControl<TMsg> Control { get; }
                
                public SubSourceStreamStageLogic(SourceShape<TMsg> shape, TopicPartition topicPartition, IActorRef consumerActor,
                                           int actorNumber, IMessageBuilder<K, V, TMsg> messageBuilder, Decider decider,
                                           Action<(TopicPartition, IControl)> subSourceStartedCallback,
                                           Action<(TopicPartition, Option<ConsumeResult<K, V>>)> subSourceCancelledCallback) 
                    : base(shape)
                {
                    _shape = shape;
                    _topicPartition = topicPartition;
                    _consumerActor = consumerActor;
                    _actorNumber = actorNumber;
                    _messageBuilder = messageBuilder;
                    _decider = decider;
                    _subSourceStartedCallback = subSourceStartedCallback;
                    _requestMessages = new KafkaConsumerActorMetadata.Internal.RequestMessages(0, ImmutableHashSet.Create(topicPartition));
                    
                    Control = new SubSourceStreamPromiseControl(shape, Complete, SetKeepGoing, GetAsyncCallback, (message, args) => Log.Debug(message, args), 
                                                                actorNumber, topicPartition, CompleteStage);
                    
                    SetHandler(shape.Outlet, onPull: Pump, onDownstreamFinish: () =>
                    {
                        var firstUnconsumed = _buffer.TryDequeue(out var message) ? new Option<ConsumeResult<K, V>>(message) : Option<ConsumeResult<K, V>>.None;
                        subSourceCancelledCallback((topicPartition, firstUnconsumed));
                        CompleteStage();
                    });
                }

                public override void PreStart()
                {
                    Log.Debug("{0} Starting SubSource for partition {1}", _actorNumber, _topicPartition);
                    
                    base.PreStart();

                    _subSourceStartedCallback((_topicPartition, Control));
                    _subSourceActor = GetStageActor(args =>
                    {
                        var (actor, message) = args;

                        switch (message)
                        {
                            case KafkaConsumerActorMetadata.Internal.Messages<K, V> messages:
                                _requested = false;
                                
                                foreach (var consumerMessage in messages.MessagesList)
                                    _buffer.Enqueue(consumerMessage);

                                Pump();
                                break;
                            case Status.Failure failure:
                                var exception = failure.Cause;
                                switch (_decider(failure.Cause))
                                {
                                    case Directive.Stop:
                                        // Throw
                                        FailStage(exception);
                                        break;
                                    case Directive.Resume:
                                        // keep going
                                        break;
                                    case Directive.Restart:
                                        // TODO: Need to do something here: https://github.com/akkadotnet/Akka.Streams.Kafka/issues/33
                                        break;
                                }
                                break;
                            case Terminated terminated when terminated.ActorRef.Equals(_consumerActor):
                                FailStage(new ConsumerFailed());
                                break;
                        }
                    });
                    
                    _subSourceActor.Watch(_consumerActor);
                }

                public override void PostStop()
                {
                    Control.OnShutdown();
                    
                    base.PostStop();
                }

                private void Pump()
                {
                    if (IsAvailable(_shape.Outlet))
                    {
                        if (_buffer.TryDequeue(out var message))
                        {
                            Push(_shape.Outlet, _messageBuilder.CreateMessage(message));
                            Pump();
                        }
                        else if (!_requested)
                        {
                            _requested = true;
                            _consumerActor.Tell(_requestMessages, _subSourceActor.Ref);
                        }
                    }
                }

                private class SubSourceStreamPromiseControl : PromiseControl<TMsg>
                {
                    private readonly ILoggingAdapter _log;
                    private readonly Action<string, object[]> _debugLog;
                    private readonly int _actorNumber;
                    private readonly TopicPartition _topicPartition;
                    private readonly Action _completeStage;

                    public SubSourceStreamPromiseControl(SourceShape<TMsg> shape, Action<Outlet<TMsg>> completeStageOutlet, 
                                                         Action<bool> setStageKeepGoing, Func<Action, Action> asyncCallbackFactory,
                                                         Action<string, object[]> debugLog, int actorNumber, 
                                                         TopicPartition topicPartition, Action completeStage) 
                        : base(shape, completeStageOutlet, setStageKeepGoing, asyncCallbackFactory)
                    {
                        _debugLog = debugLog;
                        _actorNumber = actorNumber;
                        _topicPartition = topicPartition;
                        _completeStage = completeStage;
                    }

                    public override void PerformShutdown()
                    {
                        _debugLog("#{0} Completing SubSource for partition {1}", new object[] { _actorNumber, _topicPartition });
                        _completeStage();
                    }
                }
            }
        }
    }
}