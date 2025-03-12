using System;
using System.Collections.Generic;
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
using Akka.Streams.Supervision;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Decider = Akka.Streams.Supervision.Decider;
using static Akka.Streams.Kafka.Stages.Consumers.Abstract.SubSourceLogic;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    internal static class SubSourceLogic
    {
        public sealed class CloseRevokedPartitions
        {
            public static readonly CloseRevokedPartitions Instance = new();

            private CloseRevokedPartitions()
            {
            }
        }

        /// <summary>
        /// Used to determine how the <see cref="SubSourceLogic{K,V,TMessage}"/> will handle the cancellation of
        /// a sub source stage. The default behavior requested by the <see cref="SubSourceStageLogic{K,V,TMessage}"/> is to ask
        /// the consumer to seek to the last committed offset and then re-emit the sub source stage downstream.
        /// </summary>
        internal interface ISubSourceCancellationStrategy;

        internal sealed record SeekToOffsetAndReEmit(long Offset) : ISubSourceCancellationStrategy;

        internal sealed class ReEmit : ISubSourceCancellationStrategy
        {
            public static readonly ISubSourceCancellationStrategy Instance = new ReEmit();

            private ReEmit()
            {
            }
        }

        internal sealed class DoNothing : ISubSourceCancellationStrategy
        {
            public static readonly ISubSourceCancellationStrategy Instance = new DoNothing();

            private DoNothing()
            {
            }
        }

        /// <summary>
        /// INTERNAL API
        /// </summary>
        /// <param name="Control">Control for SubSourceStageLogic</param>
        /// <param name="StageActor">Actor for the SubSourceStageLogic</param>
        public sealed record ControlAndStageActor(IControl Control, IActorRef StageActor);

        public sealed record SubSourceStageLogicControl(
            TopicPartition TopicPartition,
            ControlAndStageActor ControlAndStageActor,
            Action<IImmutableSet<TopicPartitionOffset>> FilterRevokedPartitionsCb);

        /// <summary>
        /// Factory method to create a <see cref="SubSourceStageLogic{K,V,TMessage}"/> within
        /// <see cref="SubSourceLogic{K,V,TMessage}"/> where the context parameters exist.
        /// </summary>
        public interface ISubSourceStageLogicFactory<K, V, TMessage>
        {
            SubSourceStageLogic<K, V, TMessage> Create(SourceShape<TMessage> shape, TopicPartition tp,
                IActorRef consumerActor,
                Action<SubSourceStageLogicControl> subSourceStartedCb,
                Action<(TopicPartition partition, ISubSourceCancellationStrategy cancellationStrategy)>
                    subSourceCancelledCb,
                int actorNumber);
        }
    }

    /// <summary>
    /// Stage logic used to produce sub-sources per topic partitions
    /// </summary>
    internal class SubSourceLogic<K, V, TMessage> : TimerGraphStageLogic
    {
        private readonly SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> _shape;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly IAutoSubscription _subscription;
        private readonly ISubSourceStageLogicFactory<K, V, TMessage> _subSourceStageLogicFactory;

        private readonly Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>>
            _getOffsetsOnAssign;

        private readonly Action<IImmutableSet<TopicPartition>> _onRevoke;

        private readonly int _actorNumber = KafkaConsumerActorMetadata.NextNumber();
        private readonly Action<IImmutableSet<TopicPartition>> _partitionAssignedCallback;
        private readonly Action<IImmutableSet<TopicPartition>> _updatePendingPartitionsAndEmitSubSourcesCallback;
        private readonly Action<IImmutableSet<TopicPartitionOffset>> _partitionRevokedCallback;
        private readonly Action<(TopicPartition, ISubSourceCancellationStrategy)> _subsourceCancelledCallback;
        private readonly Action<SubSourceStageLogicControl> _subsourceStartedCallback;

        private readonly Action<(IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>)>
            _offsetsFromExternalResponseCb;

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

        private IImmutableDictionary<TopicPartition, SubSourceStageLogicControl> _subSources =
            ImmutableDictionary<TopicPartition,SubSourceStageLogicControl>.Empty;

        /// <summary>
        /// Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking.
        /// </summary>
        private IImmutableSet<TopicPartition> _partitionsToRevoke = ImmutableHashSet<TopicPartition>.Empty;

        protected StageActor SourceActor { get; private set; } = null!;
        public IActorRef ConsumerActor { get; private set; } = null!;

        public PromiseControl<(TopicPartition, Source<TMessage, NotUsed>)> Control { get; }
        
        public SubSourceLogic(SourceShape<(TopicPartition, Source<TMessage, NotUsed>)> shape,
            ConsumerSettings<K, V> settings,
            IAutoSubscription subscription,
            Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> getOffsetsOnAssign,
            Action<IImmutableSet<TopicPartition>> onRevoke,
            ISubSourceStageLogicFactory<K, V, TMessage> subSourceStageLogicFactory,
            Attributes attributes)
            : base(shape)
        {
            _shape = shape;
            _settings = settings;
            _subscription = subscription;
            _subSourceStageLogicFactory = subSourceStageLogicFactory;
            _getOffsetsOnAssign = getOffsetsOnAssign;
            _onRevoke = onRevoke;

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>();
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.StoppingDecider;

            Control = new SubSourcePromiseControl(_shape, Complete, SetKeepGoing, GetAsyncCallback, GetAsyncCallback,
                PerformStop, PerformShutdown);

            _updatePendingPartitionsAndEmitSubSourcesCallback =
                GetAsyncCallback<IImmutableSet<TopicPartition>>(UpdatePendingPartitionsAndEmitSubSources);
            _partitionAssignedCallback = GetAsyncCallback<IImmutableSet<TopicPartition>>(HandlePartitionsAssigned);
            _partitionRevokedCallback = GetAsyncCallback<IImmutableSet<TopicPartitionOffset>>(HandlePartitionsRevoked);
            _stageFailCallback = GetAsyncCallback<ConsumerFailed>(FailStage);
            _subsourceCancelledCallback =
                GetAsyncCallback<(TopicPartition, ISubSourceCancellationStrategy)>(HandleSubsourceCancelled);
            _subsourceStartedCallback = GetAsyncCallback<SubSourceStageLogicControl>(HandleSubsourceStarted);
            _offsetsFromExternalResponseCb =
                GetAsyncCallback<(IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>)>(
                    OffsetsFromExternalResponseCallback);

            SetHandler(shape.Outlet, onPull: EmitSubSourcesForPendingPartitions, onDownstreamFinish: PerformShutdown);
        }

        protected void ConfigureSubscription(Action<IImmutableSet<TopicPartition>> partitionsAssignedCb,
            Action<IImmutableSet<TopicPartitionOffset>> partitionsRevokedCb)
        {
            switch (_subscription)
            {
                case TopicSubscription topicSubscription:
                    ConsumerActor.Tell(
                        new KafkaConsumerActorMetadata.Internal.Subscribe(topicSubscription.Topics,
                            AddToPartitionAssignmentHandler(CreateRebalanceListener(topicSubscription))),
                        SourceActor.Ref);
                    break;
                case TopicSubscriptionPattern topicSubscriptionPattern:
                    ConsumerActor.Tell(
                        new KafkaConsumerActorMetadata.Internal.SubscribePattern(topicSubscriptionPattern.TopicPattern,
                            AddToPartitionAssignmentHandler(CreateRebalanceListener(topicSubscriptionPattern))),
                        SourceActor.Ref);
                    break;
                default:
                    throw new NotSupportedException();
            }

            return;

            IPartitionEventHandler CreateRebalanceListener(IAutoSubscription subscription)
            {
                return new PartitionEventHandlers.Chain(
                    subscription.PartitionEventsHandler.GetOrElse(PartitionEventHandlers.Empty.Instance),
                    new PartitionEventHandlers.AsyncCallbacks(subscription, SourceActor.Ref, partitionsAssignedCb,
                        partitionsRevokedCb));
            }
        }

        private class FlushMessagesOfRevokedPartitionsHandler(SubSourceLogic<K, V, TMessage> stageLogic)
            : IPartitionEventHandler
        {
            private IImmutableSet<TopicPartitionOffset> _lastRevoked = ImmutableHashSet<TopicPartitionOffset>.Empty;

            public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
                IRestrictedConsumer consumer)
            {
                _lastRevoked = revokedTopicPartitions;
            }

            public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
            {
                foreach (var tp in revokedTopicPartitions)
                {
                    if (stageLogic._subSources.TryGetValue(tp.TopicPartition, out var control))
                        control.FilterRevokedPartitionsCb(revokedTopicPartitions);
                }
            }

            public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
            {
                // remove all of our previous revoked partitions that are not in the new assignment
                var safeToRevoke = _lastRevoked
                    .Where(c => !assignedTopicPartitions.Contains(c.TopicPartition))
                    .ToImmutableHashSet();

                foreach (var tp in safeToRevoke)
                {
                    if(stageLogic._subSources.TryGetValue(tp.TopicPartition, out var control))
                        control.FilterRevokedPartitionsCb(safeToRevoke);
                }
            }

            public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer){}
        }

        private IPartitionEventHandler AddToPartitionAssignmentHandler(IPartitionEventHandler handler)
        {
            return new PartitionEventHandlers.Chain(handler, new FlushMessagesOfRevokedPartitionsHandler(this));
        }

        public override void PreStart()
        {
            base.PreStart();
            Log.Info("Starting");
            
            SourceActor = GetStageActor(args =>
            {
                switch (args.Item2)
                {
                    case Status.Failure failure:
                        FailStage(failure.Cause);
                        break;

                    case Terminated terminated when terminated.ActorRef.Equals(ConsumerActor):
                        FailStage(new ConsumerFailed());
                        break;
                    case not null:
                        Log.Warning("Ignoring message [{0}]", args.Item2);
                        break;
                }
            });

            if (Materializer is not ActorMaterializer actorMaterializer)
                throw new ArgumentException($"Expected {typeof(ActorMaterializer)} but got {Materializer.GetType()}");

            var statisticsHandler = _subscription.StatisticsHandler.HasValue
                ? _subscription.StatisticsHandler.Value
                : StatisticsHandlers.Empty.Instance;

            var extendedActorSystem = actorMaterializer.System.AsInstanceOf<ExtendedActorSystem>();
            ConsumerActor = extendedActorSystem.SystemActorOf(
                KafkaConsumerActorMetadata.GetProps(SourceActor.Ref, _settings, _decider, statisticsHandler),
                $"kafka-consumer-{_actorNumber}");

            SourceActor.Watch(ConsumerActor);

            ConfigureSubscription(_partitionAssignedCallback, _partitionRevokedCallback);
        }

        public override void PostStop()
        {
            ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, SourceActor.Ref);

            Control.OnShutdown();

            base.PostStop();
        }

        protected override void OnTimer(object timerKey)
        {
            if (timerKey is CloseRevokedPartitions)
            {
                if(Log.IsDebugEnabled)
                    Log.Debug("#{0} Closing SubSources for revoked partitions: {1}", _actorNumber,
                        _partitionsToRevoke.JoinToString(", "));

                _onRevoke(_partitionsToRevoke);
                _pendingPartitions = _pendingPartitions.Except(_partitionsToRevoke);
                _partitionsInStartup = _partitionsInStartup.Except(_partitionsToRevoke);
                _partitionsToRevoke.ForEach(tp =>
                {
                    if (_subSources.TryGetValue(tp, out var source))
                        source.ControlAndStageActor.Control.Shutdown(PartitionWasRevoked.Instance);
                });
                _subSources = _subSources.RemoveRange(_partitionsToRevoke);
                _partitionsToRevoke = ImmutableHashSet<TopicPartition>.Empty;
            }
            else
            {
                Log.Warning("Unexpected timer [{0}]", timerKey);
            }
        }

        private void HandlePartitionsAssigned(IImmutableSet<TopicPartition> assigned)
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
                _updatePendingPartitionsAndEmitSubSourcesCallback(formerlyUnknown);
            }
            else
            {
                _getOffsetsOnAssign.Value(assigned).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        _stageFailCallback(new ConsumerFailed(
                            $"{_actorNumber} Failed to fetch offset for partitions: {formerlyUnknown.JoinToString(", ")}",
                            t.Exception));
                    }
                    else
                    {
                        _offsetsFromExternalResponseCb((formerlyUnknown, t.Result));
                    }
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        private void OffsetsFromExternalResponseCallback(
            (IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>) result)
        {
            var (formerlyUnknown, offsets) = result;
            var updatedFormerlyUnknown =
                formerlyUnknown.Except(_partitionsToRevoke.Union(_partitionsInStartup).Union(_pendingPartitions));
            SeekAndEmitSubSources(updatedFormerlyUnknown,
                offsets.Where(o => !_partitionsToRevoke.Contains(o.TopicPartition)).ToImmutableHashSet());
        }

        private void SeekAndEmitSubSources(IImmutableSet<TopicPartition> formerlyUnknown,
            IImmutableSet<TopicPartitionOffset> offsets)
        {
            _ = AskToSeekOffsets();
            return;

            async Task AskToSeekOffsets()
            {
                try
                {
                    await ConsumerActor.Ask(new KafkaConsumerActorMetadata.Internal.Seek(offsets),
                        TimeSpan.FromSeconds(10));
                    _updatePendingPartitionsAndEmitSubSourcesCallback(formerlyUnknown);
                }
                catch (Exception)
                {
                    // only exceptions that can be thrown here are related to TCS cancellation / timeout
                    _stageFailCallback(new ConsumerFailed(
                        $"{_actorNumber} Consumer failed during seek, Ask timed out. Partitions: {offsets.JoinToString(", ")}"));
                }
            }
        }

        private void HandlePartitionsRevoked(IImmutableSet<TopicPartitionOffset> revoked)
        {
            _partitionsToRevoke = _partitionsToRevoke.Union(revoked.Select(r => r.TopicPartition));

            ScheduleOnce(CloseRevokedPartitions.Instance, _settings.WaitClosePartition);
        }

        private void HandleSubsourceCancelled((TopicPartition, ISubSourceCancellationStrategy) obj)
        {
            var (topicPartition, cancellationStrategy) = obj;

            _subSources = _subSources.Remove(topicPartition);
            _partitionsInStartup = _partitionsInStartup.Remove(topicPartition);

            switch (cancellationStrategy)
            {
                case SeekToOffsetAndReEmit seek:
                    var offset = seek.Offset;
                    // re-add this partition to pending partitions so it can be re-emitted
                    _pendingPartitions = _pendingPartitions.Add(topicPartition);
                    if (Log.IsDebugEnabled)
                        Log.Debug("#{0} Seeking {1} to {2} after partition SubSource cancelled", _actorNumber,
                            topicPartition, offset);
                    var topicPartitionOffset = new TopicPartitionOffset(topicPartition, offset);
                    SeekAndEmitSubSources(
                        formerlyUnknown: ImmutableHashSet<TopicPartition>.Empty,
                        offsets: ImmutableList.Create(topicPartitionOffset).ToImmutableHashSet());
                    break;
                case ReEmit:
                    // re-add this partition to pending partitions so it can be re-emitted
                    _pendingPartitions = _pendingPartitions.Add(topicPartition);
                    EmitSubSourcesForPendingPartitions();
                    break;
                case DoNothing _:
                    break;
            }
        }

        private void HandleSubsourceStarted(SubSourceStageLogicControl sssLogicControl)
        {
            var tp = sssLogicControl.TopicPartition;
            var control = sssLogicControl.ControlAndStageActor.Control;

            if (!_partitionsInStartup.Contains(tp))
            {
                // Partition was revoked while starting up. Kill!
                control.Shutdown(PartitionWasRevoked.Instance);
            }
            else
            {
                _subSources = _subSources.SetItem(tp, sssLogicControl);
                _partitionsInStartup = _partitionsInStartup.Remove(tp);
            }
        }

        private void UpdatePendingPartitionsAndEmitSubSources(IImmutableSet<TopicPartition> formerlyUnknownPartitions)
        {
            _pendingPartitions =
                _pendingPartitions.Union(formerlyUnknownPartitions.Where(tp => !_partitionsInStartup.Contains(tp)));

            EmitSubSourcesForPendingPartitions();
        }

        private void EmitSubSourcesForPendingPartitions()
        {
            while (true)
            {
                if (_pendingPartitions.Any() && IsAvailable(_shape.Outlet))
                {
                    var topicPartition = _pendingPartitions.First();

                    _pendingPartitions = _pendingPartitions.Remove(topicPartition);
                    _partitionsInStartup = _partitionsInStartup.Add(topicPartition);

                    var subSourceStage = new SubSourceStage<K,V,TMessage>(
                        topicPartition,
                        ConsumerActor,
                        _subsourceStartedCallback,
                        _subsourceCancelledCallback,
                        _decider,
                        _actorNumber,
                        _subSourceStageLogicFactory);
                    var subsource = Source.FromGraph(subSourceStage);

                    Push(_shape.Outlet, (topicPartition, subsource));

                    continue;
                }

                break;
            }
        }

        private void PerformStop()
        {
            SetKeepGoing(true);

            _subSources.Values.Select(c => c.ControlAndStageActor.Control).ForEach(control => control.Stop());

            Complete(_shape.Outlet);

            Control.OnStop();
        }

        private void PerformShutdown(Exception? ex)
        {
            if (ex is not null and not SubscriptionWithCancelException.NonFailureCancellation)
                Log.Info(ex, $"{nameof(SubSourceLogic<K, V, TMessage>)} was shutdown due to exception");

            SetKeepGoing(true);

            // TODO from alpakka: we should wait for subsources to be shutdown and next shutdown main stage
            _subSources.Values.Select(c => c.ControlAndStageActor.Control).ForEach(control => control.Shutdown(ex));

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
                else
                {
                    Log.Warning("Ignoring message [{0}]", message);
                }
            });

            Materializer.ScheduleOnce(_settings.StopTimeout,
                () => ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance));
        }
        
        

        /// <summary>
        /// Overrides some method of base <see cref="PromiseControl{TSourceOut}"/>
        /// </summary>
        protected class SubSourcePromiseControl : PromiseControl<(TopicPartition, Source<TMessage, NotUsed>)>
        {
            private readonly Action _performStop;
            private readonly Action<Exception?> _performShutdown;

            public SubSourcePromiseControl(SourceShape<(
                    TopicPartition,
                    Source<TMessage, NotUsed>)> shape,
                Action<Outlet<(TopicPartition, Source<TMessage, NotUsed>)>> completeStageOutlet,
                Action<bool> setStageKeepGoing,
                Func<Action, Action> asyncCallbackFactory,
                Func<Action<Exception?>, Action<Exception?>> asyncShutdownCallbackFactory,
                Action performStop,
                Action<Exception?> performShutdown)
                : base(shape, completeStageOutlet, setStageKeepGoing, asyncCallbackFactory,
                    asyncShutdownCallbackFactory)
            {
                _performStop = performStop;
                _performShutdown = performShutdown;
            }

            /// <inheritdoc />
            public override void PerformStop() => _performStop();

            /// <inheritdoc />
            public override void PerformShutdown(Exception? ex) => _performShutdown(ex);
        }
    }

    /// <summary>
    /// INTERNAL API
    /// </summary>
    /// <remarks>
    /// A <see cref="SubSourceStage{K,V,TMessage}"/> is created per partition in <see cref="SubSourceLogic{K,V,TMessage}"/>
    /// </remarks>
    internal sealed class SubSourceStage<K, V, TMessage> : GraphStage<SourceShape<TMessage>>
    {
        private readonly TopicPartition _topicPartition;
        private readonly IActorRef _consumerActor;
        private readonly Action<SubSourceStageLogicControl> _subSourceStartedCallback;
        private readonly Action<(TopicPartition, ISubSourceCancellationStrategy)> _subSourceCancelledCallback;
        private readonly ISubSourceStageLogicFactory<K, V, TMessage> _subSourceStageLogicFactory;
        private readonly int _actorNumber;
        private readonly Decider _decider;

        public Outlet<TMessage> Out { get; }
        public override SourceShape<TMessage> Shape { get; }

        public SubSourceStage(TopicPartition topicPartition, IActorRef consumerActor,
            Action<SubSourceStageLogicControl> subSourceStartedCallback,
            Action<(TopicPartition, ISubSourceCancellationStrategy)> subSourceCancelledCallback,
            Decider decider,
            int actorNumber, ISubSourceStageLogicFactory<K, V, TMessage> subSourceStageLogicFactory)
        {
            _topicPartition = topicPartition;
            _consumerActor = consumerActor;
            _subSourceStartedCallback = subSourceStartedCallback;
            _subSourceCancelledCallback = subSourceCancelledCallback;
            _decider = decider;
            _actorNumber = actorNumber;
            _subSourceStageLogicFactory = subSourceStageLogicFactory;

            Out = new Outlet<TMessage>("out");
            Shape = new SourceShape<TMessage>(Out);
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return _subSourceStageLogicFactory.Create(Shape, _topicPartition, _consumerActor, 
                _subSourceStartedCallback, _subSourceCancelledCallback, _actorNumber);
        }
    }

    /// <summary>
    /// INTERNAL API
    ///
    /// A <see cref="SubSourceStageLogic{K,V,TMessage}"/> is the <see cref="GraphStageLogic"/>
    /// of a <see cref="SubSourceStage{K,V,TMessage}"/>.
    ///
    /// This emits the actual Kafka messages downstream.
    /// </summary>
    internal abstract class SubSourceStageLogic<K, V, TMessage> : GraphStageLogic
    {
        private readonly SourceShape<TMessage> _shape;
        private readonly TopicPartition _topicPartition;
        private readonly IActorRef _consumerActor;
        private readonly int _actorNumber;
        private readonly IMessageBuilder<K, V, TMessage> _messageBuilder;
        private readonly Action<SubSourceStageLogicControl> _subSourceStartedCallback;
        private readonly KafkaConsumerActorMetadata.Internal.RequestMessages _requestMessages;
        private bool _requested = false;
        private StageActor _subSourceActor = null!;
        private Queue<ConsumeResult<K, V>> _buffer = new();
        
        public Action<IImmutableSet<TopicPartitionOffset>> FilterRevokedPartitionAsyncCallback =>
            GetAsyncCallback<IImmutableSet<TopicPartitionOffset>>(FilterRevokedPartitions);
        
        private void FilterRevokedPartitions(IImmutableSet<TopicPartitionOffset> partitions)
        {
            if (partitions.Count > 0)
            {
                Log.Debug("Filtering out messages from revoked partitions [{0}]", string.Join(", ", partitions));
                var tps = partitions.Select(tpo => tpo.TopicPartition).ToImmutableHashSet();
                
                // TODO: maybe it makes sense to look at offsets too
                
                // Thread-safe - happens inside an async callback
                _buffer = new Queue<ConsumeResult<K, V>>(_buffer.Where(m => !tps.Contains(m.TopicPartition)));
            }
        }

        public PromiseControl<TMessage> Control { get; }

        public SubSourceStageLogic(SourceShape<TMessage> shape, TopicPartition topicPartition,
            IActorRef consumerActor,
            int actorNumber, IMessageBuilder<K, V, TMessage> messageBuilder, 
            Action<SubSourceStageLogicControl> subSourceStartedCallback,
            Action<(TopicPartition, ISubSourceCancellationStrategy)> subSourceCancelledCallback)
            : base(shape)
        {
            _shape = shape;
            _topicPartition = topicPartition;
            _consumerActor = consumerActor;
            _actorNumber = actorNumber;
            _messageBuilder = messageBuilder;
            _subSourceStartedCallback = subSourceStartedCallback;
            _requestMessages =
                new KafkaConsumerActorMetadata.Internal.RequestMessages(0, ImmutableHashSet.Create(topicPartition));

            Control = new SubSourceStreamPromiseControl(
                shape: shape,
                completeStageOutlet: Complete,
                setStageKeepGoing: SetKeepGoing,
                asyncCallbackFactory: GetAsyncCallback,
                asyncShutdownCallbackFactory: GetAsyncCallback,
                debugLog: (message, args) => Log.Debug(message, args),
                actorNumber: actorNumber,
                topicPartition: topicPartition,
                completeStage: ex => CompleteStage());

            SetHandler(shape.Outlet, onPull: Pump, onDownstreamFinish: ex =>
            {
                subSourceCancelledCallback((
                    topicPartition, OnDownstreamFinishSubSourceCancellationStrategy));
                base.InternalOnDownstreamFinish(ex);
            });
        }
        
        protected virtual ISubSourceCancellationStrategy OnDownstreamFinishSubSourceCancellationStrategy => _buffer.Count > 0 ? new SeekToOffsetAndReEmit(_buffer.Peek().Offset) : ReEmit.Instance;

        public override void PreStart()
        {
            base.PreStart();
            Log.Info("{0} Starting SubSource for partition {1}", _actorNumber, _topicPartition);

            _subSourceActor = GetStageActor(MessageHandling());
            _subSourceActor.Watch(_consumerActor);
            
            var controlAndActor = new ControlAndStageActor(Control, _subSourceActor.Ref);
            var started = new SubSourceStageLogicControl(this._topicPartition, controlAndActor,
                FilterRevokedPartitionAsyncCallback);

            _subSourceStartedCallback(started);
            _consumerActor.Tell(new KafkaConsumerActorMetadata.Internal.RegisterSubStage(_requestMessages.Topics),
                _subSourceActor.Ref);
        }

        private StageActorRef.Receive MessageHandling() => args =>
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
                    FailStage(failure.Cause);
                    break;
                case Terminated terminated when terminated.ActorRef.Equals(_consumerActor):
                    FailStage(new ConsumerFailed());
                    break;
            }
        };

        public override void PostStop()
        {
            Control.OnShutdown();

            base.PostStop();
        }

        private void Pump()
        {
            while (true)
            {
                if (IsAvailable(_shape.Outlet))
                {
                    if (_buffer.Count > 0)
                    {
                        var message = _buffer.Dequeue();
                        Push(_shape.Outlet, _messageBuilder.CreateMessage(message));
                        continue;
                    }

                    if (!_requested)
                    {
                        _requested = true;
                        _consumerActor.Tell(_requestMessages, _subSourceActor.Ref);
                    }
                }

                break;
            }
        }

        private class SubSourceStreamPromiseControl : PromiseControl<TMessage>
        {
            private readonly Action<string, object[]> _debugLog;
            private readonly int _actorNumber;
            private readonly TopicPartition _topicPartition;
            private readonly Action<Exception?> _completeStage;

            public SubSourceStreamPromiseControl(
                SourceShape<TMessage> shape,
                Action<Outlet<TMessage>> completeStageOutlet,
                Action<bool> setStageKeepGoing,
                Func<Action, Action> asyncCallbackFactory,
                Func<Action<Exception?>, Action<Exception?>> asyncShutdownCallbackFactory,
                Action<string, object[]> debugLog,
                int actorNumber,
                TopicPartition topicPartition,
                Action<Exception?> completeStage)
                : base(shape, completeStageOutlet, setStageKeepGoing, asyncCallbackFactory,
                    asyncShutdownCallbackFactory)
            {
                _debugLog = debugLog;
                _actorNumber = actorNumber;
                _topicPartition = topicPartition;
                _completeStage = completeStage;
            }

            public override void PerformShutdown(Exception? ex)
            {
                _debugLog("#{0} Completing SubSource for partition {1}", [_actorNumber, _topicPartition]);
                _completeStage(ex);
            }
        }
    }
}