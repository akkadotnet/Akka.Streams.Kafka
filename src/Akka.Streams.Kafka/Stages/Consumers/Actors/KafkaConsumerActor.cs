using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Util.Internal;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    /// <summary>
    /// Kafka consuming actor
    /// </summary>
    /// <typeparam name="K">Message key type</typeparam>
    /// <typeparam name="V">Message value type</typeparam>
    internal class KafkaConsumerActor<K, V> : ActorBase
    {
        private readonly IActorRef _owner;
        private readonly ConsumerSettings<K, V> _settings;
        /// <summary>
        /// Stores delegates for external handling of partition events
        /// </summary>
        private readonly IPartitionEventHandler<K,V> _partitionEventHandler;
        
        private ICancelable _poolCancellation;
        private Internal.Poll<K, V> _pollMessage;
        private Internal.Poll<K, V> _delayedPollMessage;
        
        /// <summary>
        /// Stores all incoming requests from consuming kafka stages
        /// </summary>
        private IImmutableDictionary<IActorRef, KafkaConsumerActorMetadata.Internal.RequestMessages> _requests 
            = ImmutableDictionary<IActorRef, KafkaConsumerActorMetadata.Internal.RequestMessages>.Empty;
        /// <summary>
        /// Stores stage actors, requesting for more messages
        /// </summary>
        private IImmutableSet<IActorRef> _requestors = ImmutableHashSet<IActorRef>.Empty;
        private ICommitRefreshing<K, V> _commitRefreshing;
        private IConsumer<K, V> _consumer;
        private readonly ILoggingAdapter _log;
        private bool _stopInProgress = false;
        private bool _delayedPoolInFlight = false;
        private RebalanceListenerBase _partitionAssignmentHandler = new EmptyRebalanceListener();
        private IImmutableSet<TopicPartition> _resumedPartitions = ImmutableHashSet<TopicPartition>.Empty;

        /// <summary>
        /// While `true`, committing is delayed.
        /// Changed by `onPartitionsRevoked` and `onPartitionsAssigned` callbacks
        /// </summary>
        private bool _rebalanceInProgress = false;
        /// <summary>
        /// Keeps commit offsets during rebalances for later commit.
        /// </summary>
        private IImmutableSet<TopicPartitionOffset> _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;
        /// <summary>
        /// Keeps commit senders that need a reply once stashed commits are made.
        /// </summary>
        private IImmutableList<IActorRef> _rebalanceCommitSenders = ImmutableArray<IActorRef>.Empty;

        /// <summary>
        /// KafkaConsumerActor
        /// </summary>
        /// <param name="owner">Owner actor to send critical failures to</param>
        /// <param name="settings">Consumer settings</param>
        /// <param name="partitionEventHandler">Partion events handler</param>
        public KafkaConsumerActor(IActorRef owner, ConsumerSettings<K, V> settings, IPartitionEventHandler<K,V> partitionEventHandler)
        {
            _owner = owner;
            _settings = settings;
            _partitionEventHandler = partitionEventHandler;
            
            _pollMessage = new Internal.Poll<K, V>(this, periodic: true);
            _delayedPollMessage = new Internal.Poll<K, V>(this, periodic: false);
            _log = Context.GetLogger();
            _commitRefreshing = CommitRefreshing.Create<K, V>(_settings.CommitRefreshInterval);
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case KafkaConsumerActorMetadata.Internal.Assign assign:
                {
                    ScheduleFirstPoolTask();
                    CheckOverlappingRequests("Assign", Sender, assign.TopicPartitions);
                    var previousAssigned = _consumer.Assignment;
                    _consumer.Assign(assign.TopicPartitions.Union(previousAssigned));
                    _commitRefreshing.AssignedPositions(assign.TopicPartitions, _consumer, _settings.PositionTimeout);
                    return true;
                }

                case KafkaConsumerActorMetadata.Internal.AssignWithOffset assignWithOffset:
                {
                    ScheduleFirstPoolTask();
                    var topicPartitions = assignWithOffset.TopicPartitionOffsets.Select(o => o.TopicPartition).ToImmutableHashSet();
                    CheckOverlappingRequests("AssignWithOffset", Sender, topicPartitions);
                    var previousAssigned = _consumer.Assignment.Select(tp => new TopicPartitionOffset(tp, new Offset(0)));
                    _consumer.Assign(assignWithOffset.TopicPartitionOffsets.Union(previousAssigned));
                    _commitRefreshing.AssignedPositions(topicPartitions, assignWithOffset.TopicPartitionOffsets);
                    return true;
                }
                    
                case KafkaConsumerActorMetadata.Internal.Commit commit when _rebalanceInProgress:
                    _rebalanceCommitStash = _rebalanceCommitStash.Union(commit.Offsets);
                    _rebalanceCommitSenders = _rebalanceCommitSenders.Add(Sender);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Commit commit:
                    _commitRefreshing.Add(commit.Offsets);
                    var replyTo = Sender;
                    Commit(commit.Offsets, msg => replyTo.Tell(msg));
                    return true;
                
                case Internal.Poll<K, V> poll:
                    ReceivePoll(poll);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.ISubscriptionRequest subscribe:
                    HandleSubscription(subscribe);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.RequestMessages requestMessages:
                    Context.Watch(Sender);
                    CheckOverlappingRequests("RequestMessages", Sender, requestMessages.Topics);
                    _requests = _requests.SetItem(Sender, requestMessages);
                    _requestors = _requestors.Add(Sender);
                    
                    // When many requestors, e.g. many partitions with committablePartitionedSource the
                    // performance is much by collecting more requests/commits before performing the poll.
                    // That is done by sending a message to self, and thereby collect pending messages in mailbox.
                    if (_requestors.Count == 1)
                    {
                        Poll();
                    }
                    else if (!_delayedPoolInFlight)
                    {
                        _delayedPoolInFlight = true;
                        Self.Tell(_delayedPollMessage);
                    }
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Seek seek:
                    seek.Offsets.ForEach(topicPartitionOffset =>
                    {
                        try
                        {
                            _consumer.Seek(topicPartitionOffset);
                        }
                        catch (Exception ex)
                        {
                            _log.Error($"Failed to seek to {topicPartitionOffset}: {ex}");
                            throw;
                        }
                    });
                    Sender.Tell(Done.Instance);
                    return true;
                    
                
                case KafkaConsumerActorMetadata.Internal.Committed committed:
                    _commitRefreshing.Committed(committed.Offsets);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Stop stop:
                    _log.Debug($"Received Stop from {Sender}, stopping");
                    Context.Stop(Self);
                    return true;
                
                case Terminated terminated:
                    _requests = _requests.Remove(terminated.ActorRef);
                    _requestors = _requestors.Remove(terminated.ActorRef);
                    return true;
                
                default:
                    return false;
            }
        }
       
        protected override void PreStart()
        {
            base.PreStart();

            try
            {
                var callbackHandler = new RebalanceListener<K, V>(_partitionEventHandler, this);
                _partitionAssignmentHandler = callbackHandler;
                
                _log.Debug($"Creating Kafka consumer with settings: {JsonConvert.SerializeObject(_settings)}");
                _consumer = _settings.CreateKafkaConsumer(consumeErrorHandler: (c, e) => ProcessError(new KafkaException(e)), 
                                                          partitionAssignedHandler: (c, tp) => _partitionAssignmentHandler.OnPartitionsAssigned(tp.ToImmutableHashSet()), 
                                                          partitionRevokedHandler: (c, tp) => _partitionAssignmentHandler.OnPartitionsRevoked(tp.ToImmutableHashSet()));
            }
            catch (Exception ex)
            {
                _owner?.Tell(new Status.Failure(ex));
                throw;
            }
        }

        protected override void PostStop()
        {
            // reply to outstanding requests is important if the actor is restarted
            foreach (var (actorRef, request) in _requests.ToTuples())
            {
                var emptyMessages = new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, ImmutableList<ConsumeResult<K, V>>.Empty);
                actorRef.Tell(emptyMessages);
            }
            
            _partitionAssignmentHandler.PostStop();
            
            _consumer.Dispose();
            
            base.PostStop();
        }

        private void HandleSubscription(KafkaConsumerActorMetadata.Internal.ISubscriptionRequest subscriptionRequest)
        {
            try
            {
                if (subscriptionRequest is KafkaConsumerActorMetadata.Internal.Subscribe subscribe)
                    _consumer.Subscribe(subscribe.Topics);
                else if (subscriptionRequest is KafkaConsumerActorMetadata.Internal.SubscribePattern subscribePattern)
                    _consumer.Subscribe(subscribePattern.TopicPattern);
                else
                    throw new NotSupportedException($"Unsupported subscription type: {subscriptionRequest.GetType()}");
                
                ScheduleFirstPoolTask();
            }
            catch (Exception ex)
            {
                ProcessError(ex);
            }
        }

        private void ScheduleFirstPoolTask()
        {
            if (_poolCancellation == null || _poolCancellation.IsCancellationRequested)
                SchedulePoolTask();
        }

        private void SchedulePoolTask()
        {
            _poolCancellation?.Cancel(); // Stop existing scheduling, if any
            
            _poolCancellation = Context.System.Scheduler.ScheduleTellOnceCancelable(_settings.PollInterval, Self, _pollMessage, Self);
        }

        private void CheckOverlappingRequests(string updateType, IActorRef fromStage, IImmutableSet<TopicPartition> topics)
        {
            // check if same topics/partitions have already been requested by someone else,
            // which is an indication that something is wrong, but it might be alright when assignments change.
            foreach (var (actorRef, request) in _requests.ToTuples())
            {
                if (!actorRef.Equals(fromStage) && request.Topics.Any(topics.Contains))
                {
                    _log.Warning($"{updateType} from topic/partition {string.Join(", ", topics)} " +
                                 $"already requested by other stage {string.Join(", ", request.Topics)}");
                    actorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, ImmutableList<ConsumeResult<K, V>>.Empty));
                    _requests = _requests.Remove(actorRef);
                }
            }
        }

        private void ReceivePoll(Internal.Poll<K, V> poll)
        {
            if (poll.Target == this)
            {
                var refreshOffsets = _commitRefreshing.RefreshOffsets;
                if (refreshOffsets.Any())
                {
                    _log.Debug($"Refreshing comitted offsets: {refreshOffsets.JoinToString(", ")}");
                    Commit(refreshOffsets, msg => Context.System.DeadLetters.Tell(msg));
                }
               
                Poll();
               
                if (poll.Periodic)
                    SchedulePoolTask();
                else
                    _delayedPoolInFlight = false;
            }
            else
            {
                // Message was enqueued before a restart - can be ignored
                _log.Debug("Ignoring Poll message with stale target ref");
            }
        }

        private void Poll()
        {
            var currentAssignment = _consumer.Assignment;
            var initialRebalanceInProcess = _rebalanceInProgress;

            try
            {
                if (_requests.IsEmpty())
                {
                    // no outstanding requests so we don't expect any messages back, but we should anyway
                    // drive the KafkaConsumer by polling to handle partition events etc.
                    PausePartitions(currentAssignment);
                    var message = _consumer.Consume(TimeSpan.FromMilliseconds(1));
                    if (message != null)
                    {
                        throw new IllegalActorStateException($"Got unexpected Kafka message: {message.ToJson()}");
                    }
                }
                else
                {
                    // resume partitions to fetch
                    IImmutableSet<TopicPartition> partitionsToFetch = _requests.Values.SelectMany(v => v.Topics).ToImmutableHashSet();
                    var resumeThese = currentAssignment.Where(partitionsToFetch.Contains).ToList();
                    var pauseThese = currentAssignment.Except(resumeThese).ToList();
                    PausePartitions(pauseThese);
                    ResumePartitions(resumeThese);
                    
                    ProcessResult(partitionsToFetch, _consumer.Consume(_settings.PollTimeout));
                }
            }
            catch (ConsumeException ex)
            {
                ProcessConsumingError(ex);
            }
            catch (Exception ex)
            {
                ProcessError(ex);
                _log.Error(ex, "Exception when polling from consumer, stopping actor: {0}", ex.ToString());
                Context.Stop(Self);
            }
             
            CheckRebalanceState(initialRebalanceInProcess);

            if (_stopInProgress)
            {
                _log.Debug("Stopping");
                Context.Stop(Self);
            }
        }

        private void ProcessResult(IImmutableSet<TopicPartition> partitionsToFetch, ConsumeResult<K,V> consumedMessage)
        {
            if (consumedMessage == null)
                return;

            var fetchedTopicPartition = consumedMessage.TopicPartition;
            if (!partitionsToFetch.Contains(fetchedTopicPartition))
            {
                throw  new ArgumentException($"Unexpected records polled. Expected one of {partitionsToFetch.JoinToString(", ")}," +
                                             $"but consumed result is {consumedMessage.ToJson()}, consumer assignment: {_consumer.Assignment.ToJson()}");
            }

            foreach (var (stageActorRef, request) in _requests.ToTuples())
            {
                // If requestor is interested in consumed topic, send him consumed result
                if (request.Topics.Contains(consumedMessage.TopicPartition))
                {
                    var messages = ImmutableList<ConsumeResult<K, V>>.Empty.Add(consumedMessage);
                    stageActorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, messages));
                    _requests = _requests.Remove(stageActorRef);
                }
            }
        }
        
        private void ProcessConsumingError(ConsumeException ex)
        {
            var error = ex.Error;
            _log.Error(error.Reason);

            if (!KafkaExtensions.IsBrokerErrorRetriable(error) && !KafkaExtensions.IsLocalErrorRetriable(error))
            {
                var exception = new KafkaException(error);
                ProcessError(exception);
            }
            else if (KafkaExtensions.IsLocalValueSerializationError(error))
            {
                var exception = new SerializationException(error.Reason);
                ProcessError(exception);
            }
            else
            {
                ProcessError(ex);
            }
        }
        
        private void ProcessError(Exception error)
        {
            var involvedStageActors = _requests.Keys.Append(_owner).ToImmutableHashSet();
            _log.Debug($"Sending failure to {involvedStageActors.JoinToString(", ")}. Error: {error}");
            foreach (var actor in involvedStageActors)
            {
                actor.Tell(new Status.Failure(error));
                _requests = _requests.Remove(actor);
            }
        }

        private void Commit(IImmutableSet<TopicPartitionOffset> commitMap, Action<object> sendReply)
        {
            try
            {
                _commitRefreshing.UpdateRefreshDeadlines(commitMap.Select(tp => tp.TopicPartition).ToImmutableHashSet());

                var watch = Stopwatch.StartNew();
                
                _consumer.Commit(commitMap);
                
                watch.Stop();
                if (watch.Elapsed >= _settings.CommitTimeWarning)
                    _log.Warning($"Kafka commit took longer than `commit-time-warning`: {watch.ElapsedMilliseconds} ms");

                Self.Tell(new KafkaConsumerActorMetadata.Internal.Committed(commitMap));
                sendReply(Akka.Done.Instance);
            }
            catch (Exception ex)
            {
                sendReply(new Status.Failure(ex));
            }

            // When many requestors, e.g. many partitions with committablePartitionedSource the
            // performance is much by collecting more requests/commits before performing the poll.
            // That is done by sending a message to self, and thereby collect pending messages in mailbox.
            if (_requestors.Count == 1)
            {
                Poll();
            }
            else if (!_delayedPoolInFlight)
            {
                _delayedPoolInFlight = true;
                Self.Tell(_delayedPollMessage);
            }
        }

        /// <summary>
        /// Detects state changes of <see cref="_rebalanceInProgress"/> and takes action on it.
        /// </summary>
        private void CheckRebalanceState(bool initialRebalanceInProgress)
        {
            if (initialRebalanceInProgress && !_rebalanceInProgress && _rebalanceCommitSenders.Any())
            {
                _log.Debug($"Comitting stash {string.Join(", ", _rebalanceCommitStash)} replying to {string.Join(", ", _rebalanceCommitSenders)}");
                var replyTo = _rebalanceCommitSenders;
                Commit(_rebalanceCommitStash, msg => replyTo.ForEach(actor => actor.Tell(msg)));
                _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;
                _rebalanceCommitSenders = ImmutableList<IActorRef>.Empty;
            }
        }

        private void PausePartitions(List<TopicPartition> partitions)
        {
            _consumer.Pause(partitions);
            _resumedPartitions = _resumedPartitions.Except(partitions);
        }

        private void ResumePartitions(List<TopicPartition> partitions)
        {
            var partitionsToResume = partitions.Except(_resumedPartitions).ToList();
            _consumer.Resume(partitionsToResume);
            _resumedPartitions = _resumedPartitions.Union(partitionsToResume);
        }

        class Internal
        {
            public class Poll<k, V>
            {
                public Poll(KafkaConsumerActor<K, V> target, bool periodic)
                {
                    Target = target;
                    Periodic = periodic;
                }

                public KafkaConsumerActor<K, V> Target { get; }
                public bool Periodic { get; }
            }
        }

        /// <summary>
        /// Empty implementation of <see cref="RebalanceListenerBase"/>
        /// </summary>
        class EmptyRebalanceListener : RebalanceListenerBase
        {
            public override void OnPartitionsAssigned(IImmutableSet<TopicPartition> partitions) { }

            public override void OnPartitionsRevoked(IImmutableSet<TopicPartitionOffset> partitions) { }
        }
        
        /// <summary>
        /// Implements logic for partition rebalance events. <see cref="RebalanceListenerBase"/>
        /// </summary>
        /// <remarks>
        /// TODO: Refactor this class to not use actor's private fields
        /// </remarks>
        class RebalanceListener<K, V> : RebalanceListenerBase
        {
            private readonly IPartitionEventHandler<K,V> _partitionEventHandler;
            private readonly KafkaConsumerActor<K, V> _actor;

            private readonly RestrictedConsumer<K, V> _restrictedConsumer;
            private readonly TimeSpan _warningDuration;

            public RebalanceListener(IPartitionEventHandler<K,V> partitionEventHandler, KafkaConsumerActor<K, V> actor)
            {
                _partitionEventHandler = partitionEventHandler;
                _actor = actor;

                var restrictedConsumerTimeoutMs = Math.Round(actor._settings.PartitionHandlerWarning.TotalMilliseconds * 0.95);
                _restrictedConsumer = new RestrictedConsumer<K, V>(actor._consumer, TimeSpan.FromMilliseconds(restrictedConsumerTimeoutMs));
                _warningDuration = actor._settings.PartitionHandlerWarning;
            }

            public override void OnPartitionsAssigned(IImmutableSet<TopicPartition> partitions)
            {
                var assignment = _actor._consumer.Assignment;
                var partitionsToPause = partitions.Where(p => assignment.Contains(p)).ToList();
                _actor.PausePartitions(partitionsToPause);
                
                _actor._commitRefreshing.AssignedPositions(partitions, _actor._consumer, _actor._settings.PositionTimeout);

                var watch = Stopwatch.StartNew();
                _partitionEventHandler.OnAssign(partitions, _restrictedConsumer);
                watch.Stop();
                CheckDuration(watch, "onAssign");
                
                _actor._rebalanceInProgress = false;
            }

            public override void OnPartitionsRevoked(IImmutableSet<TopicPartitionOffset> partitions)
            {
                var watch = Stopwatch.StartNew();
                _partitionEventHandler.OnRevoke(partitions, _restrictedConsumer);
                watch.Stop();
                CheckDuration(watch, "onRevoke");
                
                _actor._commitRefreshing.Revoke(partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet());
                _actor._rebalanceInProgress = true;
            }

            public override void PostStop()
            {
                var currentTopicPartitions = _actor._consumer.Assignment;
                _actor.PausePartitions(currentTopicPartitions);
                
                var watch = Stopwatch.StartNew();
                _partitionEventHandler.OnStop(currentTopicPartitions.ToImmutableHashSet(), _restrictedConsumer);
                watch.Stop();
                CheckDuration(watch, "onStop");
            }

            private void CheckDuration(Stopwatch watch, string method)
            {
                if (watch.Elapsed > _warningDuration)
                {
                    _actor._log.Warning("Partition assignment handler `{0}` took longer than `partition-handler-warning`: {1} ms", method, watch.ElapsedMilliseconds);
                }
            }
        }
    }
}