// -----------------------------------------------------------------------
//  <copyright file="KafkaConsumerActor.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

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
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Newtonsoft.Json;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;
using static Akka.Streams.Kafka.Stages.Consumers.Actors.KafkaConsumerActorMetadata.Internal;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors;

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
    
    /// <summary>
    /// Optional function to provide custom offsets during partition assignment
    /// </summary>
    private Func<IImmutableSet<TopicPartition>, IImmutableSet<TopicPartitionOffset>>? _offsetProvider;

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
    private IImmutableDictionary<IActorRef, RequestMessages> _requests
        = ImmutableDictionary<IActorRef, RequestMessages>.Empty;

    /// <summary>
    /// Stores stage actors, requesting for more messages
    /// </summary>
    private ImmutableDictionary<IImmutableSet<TopicPartition>, IActorRef> _stageActorsMap =
        ImmutableDictionary<IImmutableSet<TopicPartition>, IActorRef>.Empty;

    private ICommitRefreshing<K, V> _commitRefreshing = null!;
    private IConsumer<K, V> _consumer = null!;
    private int _commitsInProgress = 0;
    private RestrictedConsumer<K, V> _restrictedConsumer = null!;
    private IActorRef _connectionCheckerActor = null!;
    private readonly ILoggingAdapter _log;
    private bool _delayedPollInFlight = false;
    private readonly Decider _decider;
    private bool _stopInProgress;

    /// <summary>
    /// Collect commit offset maps until the next poll
    /// </summary>
    private IImmutableList<TopicPartitionOffset> _commitMaps =
        ImmutableList<TopicPartitionOffset>.Empty;

    /// <summary>
    /// Keep commit senders that need a reply once stashed commits are made
    /// </summary>
    private ImmutableHashSet<IActorRef> _commitSenders = ImmutableHashSet<IActorRef>.Empty;

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

        _pollMessage = new Internal.Poll<K, V>(this, true);
        _delayedPollMessage = new Internal.Poll<K, V>(this, false);
        _log = Context.GetLogger();
    }

    public ITimerScheduler Timers { get; set; } = null!;

    #region Rebalance listener

    private void PartitionsAssignedHandler(IImmutableSet<TopicPartition> partitions)
    {
        if (_log.IsDebugEnabled)
            _log.Debug($"Partitions were assigned: {string.Join(", ", partitions)}");
        // NOTE: we can't pause partitions here even though it's the right thing to do because the Kafka client will err out

        _commitRefreshing.AssignedPositions(partitions, _consumer, _settings.PositionTimeout);

        // clean up any unrequestedMessages belonging to revokedPartitions
        if (_unRequestedMessages.Count > 0)
        {
            // get revoked partitions that do not appear in the assigned partitions list
            var trulyRevokedPartitions = _revokedPartitions.Except(partitions);
            _unRequestedMessages = _unRequestedMessages
                .Where(m => !trulyRevokedPartitions.Contains(m.TopicPartition))
                .ToImmutableList();
        }

        var watch = Stopwatch.StartNew();
        _partitionEventHandler.OnAssign(partitions, _restrictedConsumer);
        watch.Stop();
        CheckDuration(watch, "onAssign");
    }

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
    }

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
            case Commit commit:
                // prepending as later received offsets are most likely higher
                _commitMaps = ImmutableList<TopicPartitionOffset>.Empty
                    .Add(commit.TopicPartitionOffset)
                    .AddRange(_commitMaps);
                _commitSenders = _commitSenders.Add(Sender);
                return true;

            case CommitWithoutReply commitWithoutReply:
                // prepending as later received offsets are most likely higher
                _commitMaps = ImmutableList<TopicPartitionOffset>.Empty
                    .Add(commitWithoutReply.TopicPartitionOffset)
                    .AddRange(_commitMaps);

                if (commitWithoutReply.Emergency)
                {
                    EmergencyPoll();
                }

                return true;

            case CommitSingle commitSingle:
                // prepending as later received offsets are most likely higher
                _commitMaps = ImmutableList<TopicPartitionOffset>.Empty
                    .Add(commitSingle.TopicPartitionOffset)
                    .AddRange(_commitMaps);
                _commitSenders = _commitSenders.Add(Sender);
                RequestDelayedPoll();
                return true;

            case ISubscriptionRequest subscribe:
                HandleSubscription(subscribe);
                return true;

            case RegisterSubStage subStage:
                _stageActorsMap = _stageActorsMap.SetItem(subStage.TopicPartitions, Sender);
                return true;

            case Internal.Poll<K, V> poll:
                ReceivePoll(poll);
                return true;

            case RequestMessages requestMessages:
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
                    var (requested, unrequested) =
                        _unRequestedMessages.Partition(m => requestMessages.Topics.Contains(m.TopicPartition));
                    _unRequestedMessages = unrequested;
                    if (requested.Count > 0)
                    {
                        _log.Info(
                            "Found [{0}] unrequested messages for requested partitions: {1} - [{2}] total remaining unrequested messages",
                            requested.Count, string.Join(", ", requestMessages.Topics), _unRequestedMessages.Count);
                        Sender.Tell(
                            new Messages<K, V>(requestMessages.RequestId,
                                requested.ToImmutableList()));
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

            case Seek seek:
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


            case Committed committed:
                _commitRefreshing.Committed(committed.Offsets);
                return true;

            case IStopLike s:
                var from = StopFromMessage(s);
                CommitAggregatedOffsets();
                if (_commitsInProgress == 0)
                {
                    _log.Debug("Received Stop from {0}, stopping", from);
                    Context.Stop(Self);
                }
                else
                {
                    /*
                     * Mentioned this around the `_commitsInProgress` setter, but it's very unlikely that
                     * we will ever have additional commits in progress due to the synchronous nature of
                     * committing them in the Confluent.Kafka driver for .NET.
                     *
                     * But, still a good idea to have a graceful stopping mechanism for draining them.
                     */

                    _log.Debug("Received Stop from {0}, waiting for commits in progress", from, _commitsInProgress);
                    _stopInProgress = true;
                    Context.Become(Stopping);
                }

                Context.Stop(Self);
                return true;

            case KafkaConnectionFailed kcf:
                ProcessError(kcf);
                Self.Tell(Stop.Instance);
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

    /// <summary>
    /// We only enter this behavior if we've been sent a <see cref="KafkaConsumerActorMetadata.Internal.IStopLike"/>
    /// and we still have pending commits in progress.
    /// </summary>
    private bool Stopping(object message)
    {
        // shutdown diagnostics
        LogWithPrefix();

        switch (message)
        {
            case Internal.Poll<K, V> poll:
                ReceivePoll(poll);
                return true;
            case IStopLike:
                // ignore
                return true;
            case Terminated terminated:
                _stageActorsMap = _stageActorsMap.Where(c => !c.Value.Equals(terminated.ActorRef))
                    .ToImmutableDictionary();
                return true;
            case KafkaConsumerActorMetadata.Internal.Commit or RequestMessages:
                Sender.Tell(new Status.Failure(new StoppingException()));
                return true;
            case Assign or AssignWithOffset or Subscribe or SubscribePattern:
                _log.Warning("Got unexpected message {0} wen KafkaConsumerActor is in stopping state", message);
                return true;
            default:
                return false;
        }

        void LogWithPrefix()
        {
            _log.Debug("[STOPPING] received {0}", message);
        }
    }

    private void EmergencyPoll()
    {
        _log.Debug("Performing an emergency poll");
        CommitAndPoll();
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
                (c, e) => ProcessExceptions(new KafkaException(e)),
                (c, tp) => 
                {
                    var partitions = tp.ToImmutableHashSet();
                    PartitionsAssignedHandler(partitions);
                    
                    // If we have an offset provider, use it to return custom offsets
                    if (_offsetProvider != null)
                    {
                        var customOffsets = _offsetProvider(partitions);
                        if (customOffsets != null && customOffsets.Any())
                        {
                            // Create a dictionary of custom offsets for fast lookup
                            var offsetLookup = customOffsets.ToDictionary(o => o.TopicPartition);
                            
                            // Return custom offsets where provided, Offset.Unset for others
                            return tp.Select(p => offsetLookup.TryGetValue(p, out var customOffset) 
                                ? customOffset 
                                : new TopicPartitionOffset(p, Offset.Unset));
                        }
                    }
                    
                    // Otherwise return the partitions converted to TopicPartitionOffsets with Unset offset
                    // This tells Confluent.Kafka to use the default offset behavior
                    return tp.Select(p => new TopicPartitionOffset(p, Offset.Unset));
                },
                (c, tp) => PartitionsRevokedHandler(tp.ToImmutableHashSet()),
                (c, tp) => PartitionsLostHandler(tp.ToImmutableHashSet()),
                (c, json) => _statisticsHandler.OnStatistics(c, json));

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
                _connectionCheckerActor.Tell(Stop.Instance);
            }

            // reply to outstanding requests is important if the actor is restarted
            foreach (var (actorRef, request) in _requests)
            {
                var emptyMessages = new Messages<K, V>(request.RequestId,
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

    private void HandleSubscription(ISubscriptionRequest subscriptionRequest)
    {
        try
        {
            switch (subscriptionRequest)
            {
                case Assign assign:
                {
                    CheckOverlappingRequests("Assign", Sender, assign.TopicPartitions);
                    _consumer.IncrementalAssign(assign.TopicPartitions);
                    _commitRefreshing.AssignedPositions(assign.TopicPartitions, _consumer,
                        _settings.PositionTimeout);
                    break;
                }

                case AssignWithOffset assignWithOffset:
                {
                    var topicPartitions = assignWithOffset.TopicPartitionOffsets.Select(o => o.TopicPartition)
                        .ToImmutableHashSet();
                    CheckOverlappingRequests("AssignWithOffset", Sender, topicPartitions);

                    _consumer.IncrementalAssign(assignWithOffset.TopicPartitionOffsets);
                    _commitRefreshing.AssignedPositions(topicPartitions, assignWithOffset.TopicPartitionOffsets);
                    break;
                }

                case Subscribe subscribe:
                {
                    _consumer.Subscribe(subscribe.Topics);
                    _partitionEventHandler = subscribe.RebalanceHandler;
                    _offsetProvider = subscribe.OffsetProvider;
                    break;
                }
                case SubscribePattern subscribePattern:
                {
                    _consumer.Subscribe(subscribePattern.TopicPattern);
                    _partitionEventHandler = subscribePattern.RebalanceHandler;
                    _offsetProvider = subscribePattern.OffsetProvider;
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
                actorRef.Tell(new Messages<K, V>(request.RequestId,
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
            CommitAndPoll();
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

    private void CommitAggregatedOffsets()
    {
        if (_commitMaps.Count == 0) return;
        var aggregatedOffsets = AggregateOffsets(_commitMaps);
        // commits can occur after the partition has been revoked from the consumer, so ensure that we only attempt to
        // commit partitions that are currently assigned to the consumer. For high volume topics, this can lead to small
        // amounts of replayed data during a rebalance, but for low volume topics we can ensure that consumers never appear
        // 'stuck' because of out-of-order commits from slow consumers.
        var assignedOffsetsToCommit = aggregatedOffsets.Where(kvp => _consumer.Assignment.Contains(kvp.Key))
            .Select(c => new TopicPartitionOffset(c.Key, c.Value)).ToImmutableSet();
        var replyTo = _commitSenders;
        // flush the data before calling `consumer.commit`
        _commitMaps = ImmutableList<TopicPartitionOffset>.Empty;
        _commitSenders = ImmutableHashSet<IActorRef>.Empty;
        Commit(assignedOffsetsToCommit, replyTo);
    }

    public static IReadOnlyDictionary<TopicPartition, Offset> AggregateOffsets(
        IReadOnlyCollection<TopicPartitionOffset> offsets)
    {
        var aggregate = new Dictionary<TopicPartition, Offset>();
        foreach (var offset in offsets)
        {
            if (aggregate.TryGetValue(offset.TopicPartition, out var existingOffset))
            {
                if (existingOffset < offset.Offset)
                    aggregate[offset.TopicPartition] = offset.Offset;
            }
            else
            {
                aggregate.Add(offset.TopicPartition, offset.Offset);
            }
        }

        return aggregate;
    }

    private void CommitAndPoll()
    {
        var refreshOffsets = _commitRefreshing.RefreshOffsets;
        if (refreshOffsets.Any())
        {
            _log.Debug("Refreshing committed offsets: {0}", refreshOffsets.JoinToString(", "));
            Commit(refreshOffsets, ImmutableHashSet<IActorRef>.Empty);
        }

        Poll();
    }

    private void Poll()
    {
        var currentAssignment = _consumer.Assignment.ToImmutableList();
        CommitAggregatedOffsets();
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
                {
                    /*
                     * We would normally expect a null result here, but it's totally possible for a partition
                     * assignment + a message from that partition to arrive in the same Consume(0) call.
                     * Were that to happen, we need to stash the message for future processing.
                     */
                    _unRequestedMessages = _unRequestedMessages.Add(consumed);
                    _log.Info(
                        "Received [1] message from unrequested partition: {0} - stashing for later processing. " +
                        "Total unrequested messages: {1}",
                        consumed.TopicPartition, _unRequestedMessages.Count);
                }
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
                        var (newUnrequested, requested) = polled.Partition(c =>
                            newlyAssignedButNotRequested.Contains(c.TopicPartition));
                        if (newUnrequested.Count > 0)
                        {
                            var originalUnrequestedCount = _unRequestedMessages.Count;
                            _unRequestedMessages = _unRequestedMessages.AddRange(newUnrequested);
                            var totalNewUnrequested = _unRequestedMessages.Count - originalUnrequestedCount;
                            _log.Info(
                                "Stashing [{0}] messages for newly assigned but not requested partitions: {1} - [{2}] total unrequested messages",
                                totalNewUnrequested, string.Join(", ", newlyAssignedButNotRequested),
                                _unRequestedMessages.Count);

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

        if (_stopInProgress && _commitsInProgress == 0)
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
                if (consumed is null)
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

    private void ProcessResult(IImmutableSet<TopicPartition> partitionsToFetch,
        IReadOnlyCollection<ConsumeResult<K, V>> rawResult)
    {
        if (_log.IsDebugEnabled)
            _log.Debug("Processing poll result with {0} records", rawResult.Count);

        if (rawResult.IsEmpty())
            return;

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
                    new Messages<K, V>(request.RequestId,
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

    private void Commit(IImmutableSet<TopicPartitionOffset> commitMap, IImmutableSet<IActorRef> replyTo)
    {
        var watch = Stopwatch.StartNew();
        try
        {
            _commitRefreshing.UpdateRefreshDeadlines(commitMap.Select(tp => tp.TopicPartition)
                .ToImmutableHashSet());
            _commitsInProgress += 1;

            _consumer.Commit(commitMap);
            _commitsInProgress -= 1;
            watch.Stop();
            if (watch.Elapsed >= _settings.CommitTimeWarning)
                _log.Warning(
                    $"Kafka commit took longer than `commit-time-warning`: {watch.ElapsedMilliseconds} ms");

            _commitRefreshing.Committed(commitMap);

            foreach (var s in replyTo)
            {
                s.Tell(Done.Instance);
            }
        }
        catch (KafkaException offsetException)
        {
            watch.Stop();
            switch (offsetException)
            {
                case TopicPartitionOffsetException tpoException when tpoException.Results.Any(c => c.Error.IsFatal):
                    HandleFatal(watch.Elapsed, tpoException);
                    break;
                case TopicPartitionOffsetException nonFatalTpoException: // these commits can be retried
                    RetryCommits(watch.Elapsed, nonFatalTpoException);
                    break;
                case KafkaRetriableException retriableException:
                    RetryCommits(watch.Elapsed, retriableException);
                    break;
                default:
                    HandleFatal(watch.Elapsed, offsetException);
                    break;
            }
        }
        catch (Exception ex)
        {
            watch.Stop();
            HandleFatal(watch.Elapsed, ex);
        }
        finally
        {
            _commitsInProgress -= 1;
        }

        return;

        void RetryCommits(TimeSpan duration, Exception e)
        {
            _log.Warning(e, "Kafka commit is to be retried after {0} ms, commitsInProgress={1}",
                duration.TotalMilliseconds,
                string.Join(", ", _commitsInProgress));
            _commitMaps = commitMap.ToImmutableList().AddRange(_commitMaps);
            _commitSenders = _commitSenders.Union(replyTo);
            RequestDelayedPoll();
        }

        void HandleFatal(TimeSpan duration, Exception ex)
        {
            _log.Error(ex, "Kafka commit failed after={0} ms, commitsInProgress={1}", duration.TotalMilliseconds,
                _commitsInProgress);
            var failure = new Status.Failure(ex);
            foreach (var actor in replyTo)
            {
                actor.Tell(failure);
            }
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

    private string StopFromMessage(IStopLike msg) => msg switch
    {
        Stop => Sender?.ToString() ?? "NoSender",
        StopFromStage stopFromStage => $"StageId: {stopFromStage.StageId}",
        _ => throw new ArgumentException($"Unknown message type: {msg}")
    };

    private static class Internal
    {
        public sealed class Poll<TPollKey, TPollValue>
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