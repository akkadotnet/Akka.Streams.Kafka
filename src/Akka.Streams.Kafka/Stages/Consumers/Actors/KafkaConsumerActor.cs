using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
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
    internal class KafkaConsumerActor<K, V> : ActorBase, ILogReceive
    {
        private readonly IActorRef _owner;
        private ConsumerSettings<K, V> _settings;
        /// <summary>
        /// Stores delegates for external handling of statistics
        /// </summary>
        private readonly IStatisticsHandler _statisticsHandler;

        /// <summary>
        /// Stores delegates for external handling of partition events
        /// </summary>
        private readonly IPartitionEventHandler _partitionEventHandler;
        
        private readonly RestrictedConsumer<K, V> _restrictedConsumer;
        private readonly TimeSpan _warningDuration;
        
        private ICancelable _pollCancellation;
        private readonly Internal.Poll<K, V> _pollMessage;
        private readonly Internal.Poll<K, V> _delayedPollMessage;

        private TimeSpan _pollTimeout;
        
        /// <summary>
        /// Limits the blocking on offsetForTimes
        /// </summary>
        private TimeSpan _offsetForTimesTimeout;

        private ImmutableDictionary<TopicPartition, TopicPartitionOffset> _seekedOffset = ImmutableDictionary<TopicPartition, TopicPartitionOffset>.Empty;

        /// <summary>
        /// Limits the blocking on position in [[RebalanceListenerImpl]]
        /// </summary>
        private TimeSpan _positionTimeout;

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
        private IAdminClient _adminClient;
        private IActorRef _connectionCheckerActor;
        private readonly ILoggingAdapter _log;
        private bool _stopInProgress = false;
        private bool _delayedPoolInFlight = false;
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
        /// <param name="statisticsHandler">Statistics handler</param>
        /// <param name="partitionEventHandler">Partion events handler</param>
        public KafkaConsumerActor(IActorRef owner, ConsumerSettings<K, V> settings, IPartitionEventHandler partitionEventHandler, IStatisticsHandler statisticsHandler)
        {
            _owner = owner;
            _settings = settings;
            _statisticsHandler = statisticsHandler;
            _partitionEventHandler = partitionEventHandler;
            
            var restrictedConsumerTimeoutMs = Math.Round(_settings.PartitionHandlerWarning.TotalMilliseconds * 0.95);
            _restrictedConsumer = new RestrictedConsumer<K, V>(_consumer, TimeSpan.FromMilliseconds(restrictedConsumerTimeoutMs));
            _warningDuration = _settings.PartitionHandlerWarning;
            
            _pollMessage = new Internal.Poll<K, V>(this, periodic: true);
            _delayedPollMessage = new Internal.Poll<K, V>(this, periodic: false);
            _log = Context.GetLogger();
        }

        #region Rebalance listener
        
        internal sealed class PartitionAssigned
        {
            public PartitionAssigned(IImmutableSet<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            public IImmutableSet<TopicPartition> Partitions { get; }
        }
        
        internal sealed class PartitionRevoked
        {
            public PartitionRevoked(IImmutableSet<TopicPartitionOffset> partitions)
            {
                Partitions = partitions;
            }

            public IImmutableSet<TopicPartitionOffset> Partitions { get; }
        }
    
        // This is RebalanceListener.OnPartitionAssigned on JVM
        private void PartitionsAssignedHandler(IImmutableSet<TopicPartition> partitions)
        {
            var assignment = _consumer.Assignment;
            var partitionsToPause = partitions.Where(p => assignment.Contains(p)).ToList();
            PausePartitions(partitionsToPause);
            
            _commitRefreshing.AssignedPositions(partitions, _consumer, _settings.PositionTimeout);

            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnAssign(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onAssign");
            
            _rebalanceInProgress = false;
        }

        // This is RebalanceListener.OnPartitionRevoked on JVM
        private void PartitionsRevokedHandler(IImmutableSet<TopicPartitionOffset> partitions)
        {
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnRevoke(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onRevoke");
            
            _commitRefreshing.Revoke(partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet());
            _rebalanceInProgress = true;
        }

        private void RebalancePostStop()
        {
            var currentTopicPartitions = _consumer.Assignment;
            PausePartitions(currentTopicPartitions);
            
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnStop(currentTopicPartitions.ToImmutableHashSet(), _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onStop");
        }

        private void CheckDuration(Stopwatch watch, string method)
        {
            if (watch.Elapsed > _warningDuration)
            {
                _log.Warning("Partition assignment handler `{0}` took longer than `partition-handler-warning`: {1} ms", method, watch.ElapsedMilliseconds);
            }
        }        

        #endregion
        
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
                    foreach (var offset in seek.Offsets)
                    {
                        _seekedOffset = _seekedOffset.SetItem(offset.TopicPartition, offset);
                    }
                    Sender.Tell(Done.Instance);
                    return true;
                    
                
                case KafkaConsumerActorMetadata.Internal.Committed committed:
                    _commitRefreshing.Committed(committed.Offsets);
                    return true;
                
                case KafkaConsumerActorMetadata.Internal.Stop _:
                    _log.Debug($"Received Stop from {Sender}, stopping");
                    Context.Stop(Self);
                    return true;
                
                case KafkaConnectionFailed kcf:
                    ProcessError(kcf);
                    Self.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                    return true;

                case Terminated terminated:
                    _requests = _requests.Remove(terminated.ActorRef);
                    _requestors = _requestors.Remove(terminated.ActorRef);
                    return true;

                case Metadata.IRequest req:
                    Sender.Tell(HandleMetadataRequest(req));
                    return true;
                
                // Rebalance callbacks
                case PartitionAssigned evt:
                    PartitionsAssignedHandler(evt.Partitions);
                    return true;
                
                case PartitionRevoked evt:
                    PartitionsRevokedHandler(evt.Partitions);
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
                ApplySettings(_settings);
            }
            catch (Exception ex)
            {
                _owner?.Tell(new Status.Failure(ex));
                throw;
            }
        }

        private void ApplySettings(ConsumerSettings<K, V> updatedSettings)
        {
            _settings = updatedSettings;
            _pollTimeout = _settings.PollTimeout;
            _positionTimeout = _settings.PositionTimeout;
            _commitRefreshing = CommitRefreshing.Create<K, V>(_settings.CommitRefreshInterval);
            try
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Creating Kafka consumer with settings: {JsonConvert.SerializeObject(_settings)}");

                _consumer = _settings.CreateKafkaConsumer(
                    consumeErrorHandler: (c, e) => ProcessError(new KafkaException(e)),
                    partitionAssignedHandler: (c, tp) => Self.Tell(new PartitionAssigned(tp.ToImmutableHashSet())),
                    partitionRevokedHandler: (c, tp) => Self.Tell(new PartitionRevoked(tp.ToImmutableHashSet())),
                    statisticHandler: (c, json) => _statisticsHandler.OnStatistics(c, json));

                _adminClient = new DependentAdminClientBuilder(_consumer.Handle).Build();

                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor = Context.ActorOf(ConnectionChecker.Props(_settings.ConnectionCheckerSettings));
                }
            }
            catch (Exception e)
            {
                ProcessError(e);
                throw;
            }
        }

        protected override void PostStop()
        {
            base.PostStop();
            try
            {
                _pollCancellation?.Cancel(); // Stop existing scheduling, if any
                
                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                }

                // reply to outstanding requests is important if the actor is restarted
                foreach (var (actorRef, request) in _requests.ToTuples())
                {
                    var emptyMessages = new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId,
                        ImmutableList<ConsumeResult<K, V>>.Empty);
                    actorRef.Tell(emptyMessages);
                }

                RebalancePostStop();
            }
            finally
            {
                // Make sure that the consumer is unassigned from the partition AND closed before we dispose
                try { _consumer.Unassign(); }
                catch (Exception) { /* no-op */ }

                try { _consumer.Close(); }
                catch (Exception) { /* no-op */ }

                _adminClient.Dispose();
                _consumer.Dispose();
            }
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

        private Metadata.IResponse HandleMetadataRequest(Metadata.IRequest req)
        {
            switch (req)
            {
                case Metadata.ListTopics _:
                    return new Metadata.Topics(Try<List<TopicMetadata>>
                        .From(() => _adminClient.GetMetadata(_settings.MetadataRequestTimeout).Topics));
                default:
                    throw new InvalidOperationException($"Unknown metadata request: {req}");
            }
        }

        private void ScheduleFirstPoolTask()
        {
            if (_pollCancellation == null || _pollCancellation.IsCancellationRequested)
                SchedulePoolTask();
        }

        private void SchedulePoolTask()
        {
            _pollCancellation?.Cancel(); // Stop existing scheduling, if any
            
            _pollCancellation = Context.System.Scheduler.ScheduleTellOnceCancelable(_settings.PollInterval, Self, _pollMessage, Self);
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
                    if(_log.IsDebugEnabled)
                        _log.Debug("Requests are empty - attempting to consume.");
                    PausePartitions(currentAssignment);
                    var consumed = _consumer.Consume(0);
                    if (consumed != null)
                        throw new IllegalActorStateException("Consumed message should be null");
                }
                else
                {
                    // Seek has to be done here because they can somehow fail.
                    // Would need to see if we can move this somewhere else
                    // because a seek can take up to 200ms to complete
                    foreach (var tpo in _seekedOffset.Select(kvp => kvp.Value))
                    {
                        try
                        {
                            if(_log.IsDebugEnabled)
                                _log.Debug("Seeking offset {0} in partition {1} for topic {2}", tpo.Offset, tpo.Partition, tpo.Topic);
                            _consumer.Seek(tpo);
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex, $"{tpo.TopicPartition} Failed to seek to {tpo.Offset}: {ex}");
                            throw;
                        }
                    }
                    
                    // resume partitions to fetch
                    IImmutableSet<TopicPartition> partitionsToFetch = _requests.Values.SelectMany(v => v.Topics).ToImmutableHashSet();
                    var resumeThese = currentAssignment.Where(partitionsToFetch.Contains).ToList();
                    var pauseThese = currentAssignment.Except(resumeThese).ToList();
                    PausePartitions(pauseThese);
                    ResumePartitions(resumeThese);

                    using (var cts = new CancellationTokenSource(_settings.PollTimeout))
                    {
                        ProcessResult(partitionsToFetch, PollKafka(cts.Token));
                    }
                }
            }
            // Workaroud for https://github.com/confluentinc/confluent-kafka-dotnet/issues/1366
            catch (ConsumeException ex) when (ex.Message.Contains("Broker: Unknown topic or partition") && _settings.AutoCreateTopicsEnabled)
            {
                // Trying to consume from not existing topics/partitions - assume that there are not messages to consume
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

        private List<ConsumeResult<K, V>> PollKafka(CancellationToken token)
        {
            ConsumeResult<K, V> consumed = null;
            var i = 10; // 10 poll attempts
            var timeout = Math.Max((int) _pollTimeout.TotalMilliseconds / i, 1);
            var pooled = new List<ConsumeResult<K, V>>();
            do
            {
                // this would return immediately if there are messages waiting inside the client queue buffer
                consumed = _consumer.Consume(timeout);
                if (consumed != null)
                    pooled.Add(consumed);
                i--;
            } while (i > 0 && !token.IsCancellationRequested);

            return pooled;
        }

        private void ProcessResult(IImmutableSet<TopicPartition> partitionsToFetch, List<ConsumeResult<K,V>> rawResult)
        {
            if(_log.IsDebugEnabled)
                _log.Debug("Processing poll result with {0} records", rawResult.Count);
            if(rawResult.IsEmpty())
                return;

            var fetchedTps = rawResult.Select(m => m.TopicPartition).ToImmutableSet();
            if (!fetchedTps.Except(partitionsToFetch).IsEmpty())
                throw new ArgumentException(
                    $"Unexpected records polled. Expected: [{string.Join(", ", partitionsToFetch.Select(p => p.ToString()))}], " +
                    $"result: [{string.Join(", ", fetchedTps.Select(p => p.ToString()))}], " +
                    $"consumer assignment: [{string.Join(", ", _consumer.Assignment.Select(p => p.ToString()))}]");

            //send messages to actors
            foreach (var (stageActorRef, request) in _requests.ToTuples())
            {
                var messages = new List<ConsumeResult<K, V>>();
                foreach (var message in rawResult)
                {
                    var currentTp = message.TopicPartition;
                    
                    if (_seekedOffset.TryGetValue(currentTp, out var seekedTpo))
                    {
                        if (message.Offset != seekedTpo.Offset)
                            throw new Exception("Seek failed, received message offset is greater than seek offset");
                        _seekedOffset = _seekedOffset.Remove(currentTp);
                    }
                    
                    // If requestor is interested in consumed topic, send him consumed result
                    if (request.Topics.Contains(currentTp))
                    {
                        messages.Add(message);
                    }
                }
                if(!messages.IsEmpty())
                {
                    stageActorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId, messages.ToImmutableList()));
                    _requests = _requests.Remove(stageActorRef);
                }
            }
        }
        
        private void ProcessConsumingError(ConsumeException ex)
        {
            var error = ex.Error;
            _log.Error(ex, $"ConsumerError: Code={error.Code}, Reason={error.Reason}, IsError={error.IsError}, IsFatal={error.IsFatal}");

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
            if(_log.IsDebugEnabled)
                _log.Debug("Pausing partitions [{0}]", string.Join(",", partitions));
            _consumer.Pause(partitions);
            _resumedPartitions = _resumedPartitions.Except(partitions);
        }

        private void ResumePartitions(List<TopicPartition> partitions)
        {
            var partitionsToResume = partitions.Except(_resumedPartitions).ToList();
            if(_log.IsDebugEnabled)
                _log.Debug("Resuming partitions [{0}]", string.Join(",", partitionsToResume));
            _consumer.Resume(partitionsToResume);
            _resumedPartitions = _resumedPartitions.Union(partitionsToResume);
        }

        static class Internal
        {
            public class Poll<TPollKey, TPollValue> 
                where TPollKey : K
                where TPollValue : V
            {
                public Poll(KafkaConsumerActor<TPollKey, TPollValue> target, bool periodic)
                {
                    Target = target;
                    Periodic = periodic;
                }

                public KafkaConsumerActor<TPollKey, TPollValue> Target { get; }
                public bool Periodic { get; }
            }
        }

    }
}