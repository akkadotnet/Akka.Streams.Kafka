using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Newtonsoft.Json;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    /// <summary>
    /// Kafka consuming actor
    /// </summary>
    /// <typeparam name="K">Message key type</typeparam>
    /// <typeparam name="V">Message value type</typeparam>
    internal class KafkaConsumerActor<K, V> : ActorBase, ILogReceive, IWithTimers
    {
        private const string PollTimerKey = "PollTimer";

        private readonly IActorRef? _owner;
        private ConsumerSettings<K, V> _settings;

        /// <summary>
        /// Stores delegates for external handling of statistics
        /// </summary>
        private readonly IStatisticsHandler _statisticsHandler;

        /// <summary>
        /// Stores delegates for external handling of partition events
        /// </summary>
        private IPartitionEventHandler _partitionEventHandler;

        private readonly TimeSpan _warningDuration;

        private readonly Internal.Poll<K, V> _pollMessage;
        private readonly Internal.Poll<K, V> _delayedPollMessage;

        private TimeSpan _pollTimeout;

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
        private ImmutableDictionary<IImmutableSet<TopicPartition>, IActorRef> _stageActorsMap =
            ImmutableDictionary<IImmutableSet<TopicPartition>, IActorRef>.Empty;

        private ICommitRefreshing<K, V> _commitRefreshing = null!;
        private IConsumer<K, V> _consumer = null!;
        private RestrictedConsumer<K, V> _restrictedConsumer = null!;
        private IActorRef _connectionCheckerActor = null!;
        private readonly ILoggingAdapter _log;
        private bool _stopInProgress = false;
        private bool _delayedPollInFlight = false;
        private readonly Decider _decider;

        /// <summary>
        /// While `true`, committing is delayed.
        /// Changed by `onPartitionsRevoked` and `onPartitionsAssigned` callbacks
        /// </summary>
        private bool _rebalanceInProgress = false;

        /// <summary>
        /// Keeps commit offsets during rebalances for later commit.
        /// </summary>
        private IImmutableSet<TopicPartitionOffset>
            _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;

        /// <summary>
        /// Some important behavioral difference between the .NET and Java Kafka SDKs:
        ///
        /// 1.  Pausing partitions does not guarantee that previously retrieved messages stored
        ///     inside <see cref="_consumer"/>'s buffer won't be returned.
        /// 2.  The .NET SDK works from single messages rather than batches, therefore it is totally
        ///     possible that assignment / re-balance events may occur in the middle of gathering messages
        ///     via single Consume(int) calls.
        /// 3. Therefore, it's possible that we will receive messages from partitions that have been
        ///     assigned but not requested. Therefore, we need to make sure we buffer these messages until
        ///     the <see cref="KafkaConsumerActorMetadata.Internal.RequestMessages"/> comes in.
        ///
        /// That's what this data structure is for - effectively it's a small stash of messages that have
        /// been sent but not requested. This is designed to help us prevent issues such as
        /// https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415 from prematurely terminating consumers.
        /// </summary>
        private IImmutableList<ConsumeResult<K, V>> _unRequestedMessages = ImmutableList<ConsumeResult<K, V>>.Empty;

        /// <summary>
        /// Keeps commit senders that need a reply once stashed commits are made.
        /// </summary>
        private IImmutableList<IActorRef> _rebalanceCommitSenders = ImmutableArray<IActorRef>.Empty;

        private ImmutableList<TopicPartition> _revokedPartitions = ImmutableList<TopicPartition>.Empty;

        /// <summary>
        /// KafkaConsumerActor
        /// </summary>
        /// <param name="owner">Owner actor to send critical failures to</param>
        /// <param name="settings">Consumer settings</param>
        /// <param name="decider"></param>
        /// <param name="statisticsHandler">Optional handler for reporting Confluent SDK statistics as a structured JSON payload.</param>
        public KafkaConsumerActor(IActorRef? owner, ConsumerSettings<K, V> settings, Decider decider,
            IStatisticsHandler statisticsHandler)
        {
            _owner = owner;
            _settings = settings;
            _decider = decider;
            _statisticsHandler = statisticsHandler;
            _partitionEventHandler = PartitionEventHandlers.Empty.Instance;

            _warningDuration = _settings.PartitionHandlerWarning;

            _pollMessage = new Internal.Poll<K, V>(this, periodic: true);
            _delayedPollMessage = new Internal.Poll<K, V>(this, periodic: false);
            _log = Context.GetLogger();
        }

        public ITimerScheduler Timers { get; set; } = null!;

        #region Rebalance listener

        // This is RebalanceListener.OnPartitionAssigned on JVM
        private void PartitionsAssignedHandler(IImmutableSet<TopicPartition> partitions)
        {
            if (_log.IsDebugEnabled)
                _log.Debug($"Partitions were assigned: {string.Join(", ", partitions)}");

            _commitRefreshing.AssignedPositions(partitions, _consumer, _settings.PositionTimeout);

            // clean up any unrequestedMessages belonging to revokedPartitions
            if (_unRequestedMessages.Count > 0)
            {
                // get revoked partitions that do not appear in the assigned partitions list
                var trulyRevokedPartitions = _revokedPartitions.Except(partitions);
                _unRequestedMessages = _unRequestedMessages.Where(m => !trulyRevokedPartitions.Contains(m.TopicPartition))
                    .ToImmutableList();
            }
               
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnAssign(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onAssign");

            _rebalanceInProgress = false;
        }

        // This is RebalanceListener.OnPartitionRevoked on JVM
        private void PartitionsRevokedHandler(IImmutableSet<TopicPartitionOffset> partitions)
        {
            if (_log.IsDebugEnabled)
                _log.Debug($"Partitions were revoked: {string.Join(", ", partitions)}");

            // keep track of which partitions are being revoked, so we can clean up our "unrequested messages"
            _revokedPartitions = partitions.Select(tp => tp.TopicPartition).ToImmutableList();
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnRevoke(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onRevoke");

            _commitRefreshing.Revoke(partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet());
            _rebalanceInProgress = true;
        }

        // This is RebalanceListener.OnPartitionLost on JVM
        private void PartitionsLostHandler(IImmutableSet<TopicPartitionOffset> partitions)
        {
            if (_log.IsDebugEnabled)
                _log.Debug($"Partitions were lost: {string.Join(", ", partitions)}");
            
            if (_unRequestedMessages.Count > 0)
            {
                // immediately clean up any unrequestedMessages belonging to lost partitions
                var lostPartitions = partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet();
                _unRequestedMessages = _unRequestedMessages.Where(m => !lostPartitions.Contains(m.TopicPartition))
                    .ToImmutableList();
            }
           
            var watch = Stopwatch.StartNew();
            _partitionEventHandler.OnLost(partitions, _restrictedConsumer);
            watch.Stop();
            CheckDuration(watch, "onLost");

            _commitRefreshing.Revoke(partitions.Select(tp => tp.TopicPartition).ToImmutableHashSet());
            _rebalanceInProgress = true;
        }

        private void RebalancePostStop()
        {
            var currentTopicPartitions = _consumer.Assignment.ToImmutableList();
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
                _log.Warning("Partition assignment handler `{0}` took longer than `partition-handler-warning`: {1} ms",
                    method, watch.ElapsedMilliseconds);
            }
        }

        #endregion

        protected override bool Receive(object message)
        {
            switch (message)
            {
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

                case KafkaConsumerActorMetadata.Internal.RegisterSubStage subStage:
                    _stageActorsMap = _stageActorsMap.SetItem(subStage.TopicPartitions, Sender);
                    return true;

                case KafkaConsumerActorMetadata.Internal.RequestMessages requestMessages:
                    if (_settings.VerboseLogging)
                        _log.Debug("Messages was requested, RequestId: {0}, Partitions: {1}", requestMessages.RequestId,
                            string.Join(", ", requestMessages.Topics));
                    Context.Watch(Sender);
                    CheckOverlappingRequests("RequestMessages", Sender, requestMessages.Topics);

                    if (_stageActorsMap.GetOrElse(requestMessages.Topics, Sender).Equals(Sender))
                        _requests = _requests.SetItem(Sender, requestMessages);
                    
                    /* UNREQUESTED MESSAGES CHECK */
                    if (_unRequestedMessages.Count > 0)
                    {
                        var (requested, unrequested) = _unRequestedMessages.Partition(m => requestMessages.Topics.Contains(m.TopicPartition));
                        _unRequestedMessages = unrequested;
                        if (requested.Count > 0)
                        {
                            _log.Info("Found [{0}] unrequested messages for requested partitions: {1} - [{2}] total remaining unrequested messages",
                                requested.Count, string.Join(", ", requestMessages.Topics), _unRequestedMessages.Count);
                            Sender.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(requestMessages.RequestId, requested.ToImmutableList()));
                            _requests = _requests.Remove(Sender);
                            
                            // not going to schedule a poll of any kind here - wait until we receive the next request
                            return true;
                        }
                    }

                    // When many requestors, e.g. many partitions with committablePartitionedSource the
                    // performance is much by collecting more requests/commits before performing the poll.
                    // That is done by sending a message to self, and thereby collect pending messages in mailbox.
                    if (_stageActorsMap.Count == 1)
                    {
                        Poll();
                    }
                    else
                    {
                        RequestDelayedPoll();
                    }

                    return true;

                case KafkaConsumerActorMetadata.Internal.Seek seek:
                    try
                    {
                        foreach (var offset in seek.Offsets)
                        {
                            _consumer.Seek(offset);
                        }

                        Sender.Tell(Done.Instance);
                    }
                    catch (Exception ex)
                    {
                        SendFailure(ex, Sender);
                    }

                    return true;


                case KafkaConsumerActorMetadata.Internal.Committed committed:
                    _commitRefreshing.Committed(committed.Offsets);
                    return true;

                case KafkaConsumerActorMetadata.Internal.Stop:
                    _log.Debug($"Received Stop from {Sender}, stopping");
                    Context.Stop(Self);
                    return true;

                case KafkaConnectionFailed kcf:
                    ProcessError(kcf);
                    Self.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                    return true;

                case Terminated terminated:
                    _requests = _requests.Remove(terminated.ActorRef);
                    _stageActorsMap = _stageActorsMap.Where(c => !c.Value.Equals(terminated.ActorRef))
                        .ToImmutableDictionary();
                    return true;

                case Metadata.IRequest req:
                    Sender.Tell(HandleMetadataRequest(req));
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
                    consumeErrorHandler: (c, e) => ProcessExceptions(new KafkaException(e)),
                    partitionAssignedHandler: (c, tp) => PartitionsAssignedHandler(tp.ToImmutableHashSet()),
                    partitionRevokedHandler: (c, tp) => PartitionsRevokedHandler(tp.ToImmutableHashSet()),
                    partitionLostHandler: (c, tp) => PartitionsLostHandler(tp.ToImmutableHashSet()),
                    statisticHandler: (c, json) => _statisticsHandler.OnStatistics(c, json));

                var restrictedConsumerTimeoutMs =
                    Math.Round(_settings.PartitionHandlerWarning.TotalMilliseconds * 0.95);
                _restrictedConsumer =
                    new RestrictedConsumer<K, V>(_consumer, TimeSpan.FromMilliseconds(restrictedConsumerTimeoutMs));

                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor =
                        Context.ActorOf(ConnectionChecker.Props(_settings.ConnectionCheckerSettings));
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
                Timers.CancelAll(); // Stop existing scheduling, if any

                if (_settings.ConnectionCheckerSettings.Enabled)
                {
                    _connectionCheckerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance);
                }

                // reply to outstanding requests is important if the actor is restarted
                foreach (var (actorRef, request) in _requests)
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
                try
                {
                    _consumer.Unassign();
                }
                catch (Exception)
                {
                    /* no-op */
                }

                try
                {
                    _consumer.Close();
                }
                catch (Exception)
                {
                    /* no-op */
                }

                _consumer.Dispose();
            }
        }

        private void HandleSubscription(KafkaConsumerActorMetadata.Internal.ISubscriptionRequest subscriptionRequest)
        {
            try
            {
                switch (subscriptionRequest)
                {
                    case KafkaConsumerActorMetadata.Internal.Assign assign:
                    {
                        CheckOverlappingRequests("Assign", Sender, assign.TopicPartitions);
                        var previousAssigned = _consumer.Assignment;
                        _consumer.Assign(assign.TopicPartitions.Union(previousAssigned));
                        _commitRefreshing.AssignedPositions(assign.TopicPartitions, _consumer,
                            _settings.PositionTimeout);
                        break;
                    }

                    case KafkaConsumerActorMetadata.Internal.AssignWithOffset assignWithOffset:
                    {
                        var topicPartitions = assignWithOffset.TopicPartitionOffsets.Select(o => o.TopicPartition)
                            .ToImmutableHashSet();
                        CheckOverlappingRequests("AssignWithOffset", Sender, topicPartitions);

                        var previousAssigned = _consumer.Assignment
                            .Select(c => new TopicPartitionOffset(c, Offset.Stored));
                        _consumer.Assign(assignWithOffset.TopicPartitionOffsets.Union(previousAssigned));
                        _commitRefreshing.AssignedPositions(topicPartitions, assignWithOffset.TopicPartitionOffsets);
                        break;
                    }

                    case KafkaConsumerActorMetadata.Internal.Subscribe subscribe:
                    {
                        _consumer.Subscribe(subscribe.Topics);
                        _partitionEventHandler = subscribe.RebalanceHandler;
                        break;
                    }
                    case KafkaConsumerActorMetadata.Internal.SubscribePattern subscribePattern:
                    {
                        _consumer.Subscribe(subscribePattern.TopicPattern);
                        _partitionEventHandler = subscribePattern.RebalanceHandler;
                        break;
                    }
                }

                ScheduleFirstPollTask();
                _stageActorsMap = _stageActorsMap.SetItem(_consumer.Assignment.ToImmutableSet(), Sender);
            }
            catch (Exception ex)
            {
                // only this sender needs to be notified about the failure
                SendFailure(ex, Sender);
            }
        }

        private Metadata.IResponse HandleMetadataRequest(Metadata.IRequest req)
        {
            switch (req)
            {
                case Metadata.ListTopics _:
                    return new Metadata.Topics(Try<List<TopicMetadata>>
                        .From(() =>
                        {
                            using (var adminClient = new DependentAdminClientBuilder(_consumer.Handle).Build())
                            {
                                return adminClient.GetMetadata(_settings.MetadataRequestTimeout).Topics;
                            }
                        }));
                default:
                    throw new InvalidOperationException($"Unknown metadata request: {req}");
            }
        }

        private void ScheduleFirstPollTask()
        {
            if (!Timers.IsTimerActive(PollTimerKey))
                SchedulePollTask();
        }

        private void SchedulePollTask()
        {
            Timers.CancelAll();
            Timers.StartSingleTimer(PollTimerKey, _pollMessage, _settings.PollInterval);
        }

        private void RequestDelayedPoll()
        {
            if (_delayedPollInFlight)
            {
                _delayedPollInFlight = true;
                Self.Tell(_delayedPollMessage);
            }
        }

        private void CheckOverlappingRequests(string updateType, IActorRef fromStage,
            IImmutableSet<TopicPartition> topics)
        {
            // check if same topics/partitions have already been requested by someone else,
            // which is an indication that something is wrong, but it might be alright when assignments change.
            foreach (var (actorRef, request) in _requests)
            {
                if (!actorRef.Equals(fromStage) && request.Topics.Any(topics.Contains))
                {
                    _log.Warning($"{updateType} from topic/partition {string.Join(", ", topics)} " +
                                 $"already requested by other stage {string.Join(", ", request.Topics)}");
                    actorRef.Tell(new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId,
                        ImmutableList<ConsumeResult<K, V>>.Empty));
                    _requests = _requests.Remove(actorRef);
                }
            }
        }

        private void ReceivePoll(Internal.Poll<K, V> poll)
        {
            // We overloaded `==`, we need to use `ReferenceEquals` to do this
            if (ReferenceEquals(poll.Target, this))
            {
                var refreshOffsets = _commitRefreshing.RefreshOffsets;
                if (refreshOffsets.Any())
                {
                    _log.Debug("Refreshing committed offsets: {0}", refreshOffsets.JoinToString(", "));
                    Commit(refreshOffsets, msg => Context.System.DeadLetters.Tell(msg));
                }

                Poll();

                if (poll.Periodic)
                    SchedulePollTask();
                else
                    _delayedPollInFlight = false;
            }
            else
            {
                // Message was enqueued before a restart - can be ignored
                _log.Debug("Ignoring Poll message with stale target ref");
            }
        }

        private void Poll()
        {
            var currentAssignment = _consumer.Assignment.ToImmutableList();
            var initialRebalanceInProcess = _rebalanceInProgress;

            var partitionsToFetch = _requests.Values.SelectMany(v => v.Topics)
                .ToImmutableHashSet();

            if (_requests.IsEmpty())
            {
                if (_settings.VerboseLogging)
                    _log.Debug("Requests are empty - attempting to consume.");
                PausePartitions(currentAssignment);
                try
                {
                    var consumed = _consumer.Consume(0);
                    if (consumed is not null)
                        throw new IllegalActorStateException("Consumed message should be null");
                }
                catch (Exception e)
                {
                    ProcessExceptions(e);
                }
            }
            else
            {
                // resume partitions to fetch
                var (resumeThese, pauseThese) = currentAssignment.Partition(partitionsToFetch.Contains);
                PausePartitions(
                    pauseThese); // SHOULD PAUSE ANY PARTITIONS THAT HAVE BEEN ASSIGNED BUT ARE NOT REQUESTED
                ResumePartitions(resumeThese);

                using (var cts = new CancellationTokenSource(_settings.PollTimeout))
                {
                    var (polled, exception) = PollKafka(cts.Token);
                    var assignedAtEnd = _consumer.Assignment.ToImmutableHashSet();
                    
                    /*
                     * Any partitions that were NOT ASSIGNED at the start of the poll BUT ARE ASSIGNED NOW
                     * got assigned to us mid-poll then. We need to stash these messages until they are requested.
                     */
                    var newlyAssigned = assignedAtEnd.Except(currentAssignment);
                    if (newlyAssigned.Any())
                    {
                        // now filter again to see if any of these newlyAssigned partitions have not been requested
                        var newlyAssignedButNotRequested = newlyAssigned.Except(partitionsToFetch);
                        if (newlyAssignedButNotRequested.Any())
                        {
                            var (newUnrequested, requested) = polled.Partition(c => newlyAssignedButNotRequested.Contains(c.TopicPartition));
                            if (newUnrequested.Count > 0)
                            {
                                var originalUnrequestedCount = _unRequestedMessages.Count;
                                _unRequestedMessages = _unRequestedMessages.AddRange(newUnrequested);
                                var totalNewUnrequested = _unRequestedMessages.Count - originalUnrequestedCount;
                                _log.Info("Stashing [{0}] messages for newly assigned but not requested partitions: {1} - [{2}] total unrequested messages",
                                    totalNewUnrequested,  string.Join(", ", newlyAssignedButNotRequested), _unRequestedMessages.Count);

                                polled = requested;
                            }
                        }
                    }
                    
                    try
                    {
                        ProcessResult(partitionsToFetch, polled);
                    }
                    catch (Exception e)
                    {
                        ProcessExceptions(e);
                    }

                    if (exception is not null)
                        ProcessExceptions(exception);
                }
            }

            CheckRebalanceState(initialRebalanceInProcess);

            if (_stopInProgress)
            {
                _log.Debug("Stopping");
                Context.Stop(Self);
            }
        }

        private (IReadOnlyCollection<ConsumeResult<K, V>>, Exception?) PollKafka(CancellationToken token)
        {
            var i = _settings.MaxPollRecords; // use the number of poll attempts specified in the settings
            var timeout = Math.Max((int)_pollTimeout.TotalMilliseconds / i, 1);
            var polled = new List<ConsumeResult<K, V>>();
            do
            {
                try
                {
                    // this would return immediately if there are messages waiting inside the client queue buffer
                    var consumed = _consumer.Consume(timeout);
                    if (consumed is null || _rebalanceInProgress) // bail out if we detect the start of a re-balance
                    {
                        return (polled, null);
                    }

                    polled.Add(consumed);
                    i--;
                }
                catch (Exception e)
                {
                    return (polled, e);
                }
            } while (i > 0 && !token.IsCancellationRequested);

            return (polled, null);
        }

        private void ProcessResult(IImmutableSet<TopicPartition> partitionsToFetch, IReadOnlyCollection<ConsumeResult<K, V>> rawResult)
        {
            if (_log.IsDebugEnabled)
                _log.Debug("Processing poll result with {0} records", rawResult.Count);

            if (rawResult.IsEmpty())
                return;
            
            // TODO: remove after we verify the fix to https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415
            var fetchedTps = rawResult.Select(m => m.TopicPartition).ToImmutableSet();
            if (!fetchedTps.Except(partitionsToFetch).IsEmpty())
                throw new ArgumentException(
                    $"Unexpected records polled. Expected: [{string.Join(", ", partitionsToFetch.Select(p => p.ToString()))}], " +
                    $"result: [{string.Join(", ", fetchedTps.Select(p => p.ToString()))}], " +
                    $"consumer assignment: [{string.Join(", ", _consumer.Assignment.Select(p => p.ToString()))}]");

            //send messages to actors
            foreach (var (stageActorRef, request) in _requests)
            {
                var messages = new List<ConsumeResult<K, V>>();
                foreach (var message in rawResult)
                {
                    var currentTp = message.TopicPartition;

                    // If requestor is interested in consumed topic, send him consumed result
                    if (request.Topics.Contains(currentTp))
                    {
                        messages.Add(message);
                    }
                }

                if (!messages.IsEmpty())
                {
                    stageActorRef.Tell(
                        new KafkaConsumerActorMetadata.Internal.Messages<K, V>(request.RequestId,
                            messages.ToImmutableList()));
                    _requests = _requests.Remove(stageActorRef);
                }
            }
        }

        private void SendFailure(Exception ex, IActorRef stageActorRef)
        {
            stageActorRef.Tell(new Status.Failure(ex));
            _stageActorsMap = _stageActorsMap.Where(c => !c.Value.Equals(stageActorRef)).ToImmutableDictionary();
        }

        private void ProcessError(Exception error)
        {
            var sendTo = _stageActorsMap.Values.ToImmutableSet();
            if (_owner != null)
                sendTo = sendTo.Add(_owner);

            _log.Debug(error, "Sending failure {0} to {1}.", error.GetType(), string.Join(", ", sendTo));
            foreach (var actor in sendTo)
            {
                SendFailure(error, actor);
            }
        }

        private void Commit(IImmutableSet<TopicPartitionOffset> commitMap, Action<object> sendReply)
        {
            try
            {
                _commitRefreshing.UpdateRefreshDeadlines(commitMap.Select(tp => tp.TopicPartition)
                    .ToImmutableHashSet());

                var watch = Stopwatch.StartNew();

                _consumer.Commit(commitMap);

                watch.Stop();
                if (watch.Elapsed >= _settings.CommitTimeWarning)
                    _log.Warning(
                        $"Kafka commit took longer than `commit-time-warning`: {watch.ElapsedMilliseconds} ms");

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
            if (_stageActorsMap.Count == 1)
            {
                Poll();
            }
            else
            {
                RequestDelayedPoll();
            }
        }

        /// <summary>
        /// Detects state changes of <see cref="_rebalanceInProgress"/> and takes action on it.
        /// </summary>
        private void CheckRebalanceState(bool initialRebalanceInProgress)
        {
            if (initialRebalanceInProgress && !_rebalanceInProgress && _rebalanceCommitSenders.Any())
            {
                _log.Debug(
                    $"Comitting stash {string.Join(", ", _rebalanceCommitStash)} replying to {string.Join(", ", _rebalanceCommitSenders)}");
                var replyTo = _rebalanceCommitSenders;
                Commit(_rebalanceCommitStash, msg => replyTo.ForEach(actor => actor.Tell(msg)));
                _rebalanceCommitStash = ImmutableHashSet<TopicPartitionOffset>.Empty;
                _rebalanceCommitSenders = ImmutableList<IActorRef>.Empty;
            }
        }

        private void PausePartitions(IImmutableList<TopicPartition> partitions)
        {
            if (partitions.Count == 0)
                return;

            if (_settings.VerboseLogging)
                _log.Debug("Pausing partitions [{0}]", string.Join(",", partitions));
            _consumer.Pause(partitions);
        }

        private void ResumePartitions(IImmutableList<TopicPartition> partitions)
        {
            if (partitions.Count == 0)
                return;

            if (_settings.VerboseLogging)
                _log.Debug("Resuming partitions [{0}]", string.Join(",", partitions));
            _consumer.Resume(partitions);
        }

        private void ProcessExceptions(Exception? exception)
        {
            if (exception == null)
                return;

            var directive = _decider(exception);
            ProcessError(exception);
            if (directive == Directive.Resume)
                return;

            Timers.CancelAll();
            if (directive == Directive.Stop && _log.IsErrorEnabled)
                _log.Error(exception, "Exception when polling from consumer, stopping actor: {0}", exception.Message);
            Context.Stop(Self);
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