using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    public class StoppingException : Exception
    {
        public StoppingException() : base("Kafka consumer is stopping") { }
    }

    #region Internal Messages

    //
    // requests
    //

    public static class Internal
    {
        internal struct Assign
        {
            public Assign(IImmutableSet<TopicPartition> topicPartitions)
            {
                TopicPartitions = topicPartitions;
            }

            public IImmutableSet<TopicPartition> TopicPartitions { get; }
        }

        internal struct AssignWithOffset
        {
            public AssignWithOffset(IImmutableDictionary<TopicPartition, long> topicPartitions)
            {
                TopicPartitions = topicPartitions;
            }

            public IImmutableDictionary<TopicPartition, long> TopicPartitions { get; }
        }

        // TODO: AssignOffsetsForTimes

        internal struct Subscribe
        {
            public Subscribe(IImmutableSet<string> topics, object listener)
            {
                Topics = topics;
                Listener = listener;
            }

            public IImmutableSet<string> Topics { get; }
            public object Listener { get; }
        }

        internal struct SubscribePattern
        {
            public SubscribePattern(string topicPattern, object listener)
            {
                TopicPattern = topicPattern;
                Listener = listener;
            }

            public string TopicPattern { get; }
            public object Listener { get; }
        }

        internal struct RequestMessages
        {
            public RequestMessages(int requestId, ImmutableHashSet<TopicPartition> topics)
            {
                RequestId = requestId;
                Topics = topics;
            }

            public int RequestId { get; }
            public ImmutableHashSet<TopicPartition> Topics { get; }
        }

        internal struct Stop
        {
            public static Stop Instance { get; } = new Stop();
        }

        internal struct Commit
        {
            public Commit(ImmutableDictionary<TopicPartition, long> offsets)
            {
                Offsets = offsets;
            }

            public ImmutableDictionary<TopicPartition, long> Offsets { get; }
        }

        //
        // responses
        //

        internal struct Assigned
        {
            public Assigned(ImmutableList<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            public ImmutableList<TopicPartition> Partitions { get; }
        }

        internal struct Revoked
        {
            public Revoked(ImmutableList<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            public ImmutableList<TopicPartition> Partitions { get; }

        }

        internal struct Messages<TKey, TValue>
        {
            public Messages(int requestId, IEnumerable<Message<TKey, TValue>> kafkaMessages)
            {
                RequestId = requestId;
                KafkaMessages = kafkaMessages;
            }

            public int RequestId { get; }
            public IEnumerable<Message<TKey, TValue>> KafkaMessages { get; }
        }

        internal struct Committed
        {
            public Committed(ImmutableDictionary<TopicPartition, Offset> offsets)
            {
                Offsets = offsets;
            }

            public ImmutableDictionary<TopicPartition, Offset> Offsets { get; }
        }

    }

    #endregion

    internal struct Poll<TKey, TValue> : IDeadLetterSuppression
    {
        public Poll(KafkaConsumerActor<TKey, TValue> target)
        {
            Target = target;
        }

        public KafkaConsumerActor<TKey, TValue> Target { get; }
    }

    public static class KafkaConsumerActor
    {
        private static readonly AtomicCounter Number = new AtomicCounter(0);

        public static int NextNumber() => Number.IncrementAndGet();

        public static Actor.Props Props<TKey, TValue>(ConsumerSettings<TKey, TValue> settings) =>
            Actor.Props.Create(() => new KafkaConsumerActor<TKey, TValue>(settings)).WithDispatcher(settings.DispatcherId);
    }


    public class KafkaConsumerActor<TKey, TValue> : ReceiveActor
    {
        private readonly ConsumerSettings<TKey, TValue> _settings;
        private readonly Poll<TKey, TValue> pollMsg;

        private ICancelable currentPollTask;
        private readonly Dictionary<IActorRef, Internal.RequestMessages> requests = new Dictionary<IActorRef, Internal.RequestMessages>();
        private Confluent.Kafka.Consumer<TKey, TValue> consumer;
        private int commitsInProgress = 0;
        private int wakeUps = 0;
        private bool stopInProgress = false;

        private ILoggingAdapter log;
        public ILoggingAdapter Log => log ?? (log = Context.GetLogger());
        protected virtual TimeSpan PollTimeout => _settings.PollTimeout;
        protected virtual TimeSpan PollInterval => _settings.PollInterval;

        public KafkaConsumerActor(ConsumerSettings<TKey, TValue> settings)
        {
            _settings = settings;
            pollMsg = new Poll<TKey, TValue>(this);

            Receive<Internal.Assign>(assign =>
            {
                // TODO: scheduleFirstPollTask()
                CheckOverlappingRequests("Assign", Sender, assign.TopicPartitions);
                var previousAssigned = consumer.Assignment;
                previousAssigned.AddRange(assign.TopicPartitions);
                consumer.Assign(previousAssigned);
            });

            Receive<Internal.AssignWithOffset>(assign =>
            {
                // TODO: scheduleFirstPollTask()
                var topicPartitions = assign.TopicPartitions.Keys.ToImmutableHashSet();
                CheckOverlappingRequests("AssignWithOffset", Sender, topicPartitions);
                var assigned = consumer.Assignment;
                assigned.AddRange(topicPartitions);
                consumer.Assign(assigned);

                foreach (var entry in assign.TopicPartitions)
                {
                    // TODO: Seek is not implemented in Kafka driver
                    //consumer.Seek(entry.Key, entry.Value);
                }
            });

            Receive<Internal.Commit>(commit =>
            {
                var commitMap = commit.Offsets
                    .Select(entry => new TopicPartitionOffset(entry.Key, new Offset(entry.Value)))
                    .ToArray();
                var reply = Sender;
                commitsInProgress++;
                consumer.CommitAsync(commitMap)
                    .ContinueWith(task =>
                    {
                        // this is invoked on the thread calling consumer.poll which will always be the actor, so it is safe
                        commitsInProgress--;
                        object msg;
                        if (task.IsFaulted || task.IsCanceled)
                        {
                            msg = new Status.Failure(task.Exception);
                        }
                        else if (task.Result.Error.HasError)
                        {
                            msg = new Status.Failure(new KafkaException(task.Result.Error.Code));
                        }
                        else
                        {
                            var content = task.Result.Offsets
                                .Select(x => new KeyValuePair<TopicPartition, Offset>(x.TopicPartition, x.Offset))
                                .ToImmutableDictionary();
                            msg = new Internal.Committed(content);
                        }
                        reply.Tell(msg);
                    });
                //right now we can not store commits in consumer - https://issues.apache.org/jira/browse/KAFKA-3412
                Poll();
            });

            Receive<Internal.Subscribe>(subscribe =>
            {
                consumer.Subscribe(subscribe.Topics);
            });

            Receive<Internal.SubscribePattern>(subscribe =>
            {
                consumer.Subscribe(subscribe.TopicPattern);
            });

            Receive<Poll<TKey, TValue>>(poll =>
            {
                if (poll.Target == this)
                {
                    Poll();
                    currentPollTask = SchedulePollTask();
                }
                else Log.Debug("Ignoring Poll message with stale target ref");
            });

            Receive<Internal.RequestMessages>(request =>
            {
                Context.Watch(Sender);
                CheckOverlappingRequests(nameof(Internal.RequestMessages), Sender, request.Topics);
                requests[Sender] = request;
                Poll();
            });

            Receive<Internal.Stop>(stop =>
            {
                if (commitsInProgress == 0)
                {
                    Context.Stop(Self);
                }
                else
                {
                    stopInProgress = true;
                    Become(Stopping);
                }
            });

            Receive<Terminated>(terminated =>
            {
                requests.Remove(terminated.ActorRef);
            });
        }

        private void Stopping()
        {
            Receive<Poll<TKey, TValue>>(poll =>
            {
                if (poll.Target == this)
                {
                    Poll();
                    currentPollTask = SchedulePollTask();
                }
                else Log.Debug("Ignoring Poll message with stale target ref");
            });
            Receive<Internal.Stop>(_ => { /* ignore */ });
            Receive<Terminated>(_ => { /* ignore */ });
            Receive<Internal.Commit>(x => Sender.Tell(new Status.Failure(new StoppingException())));
            Receive<Internal.RequestMessages>(x => Sender.Tell(new Status.Failure(new StoppingException())));
            Receive<Internal.Assign>(x => Log.Warning("Got unexpected message {0} when KafkaConsumerActor is in stopping stage", x));
            Receive<Internal.AssignWithOffset>(x => Log.Warning("Got unexpected message {0} when KafkaConsumerActor is in stopping stage", x));
            Receive<Internal.Subscribe>(x => Log.Warning("Got unexpected message {0} when KafkaConsumerActor is in stopping stage", x));
            Receive<Internal.SubscribePattern>(x => Log.Warning("Got unexpected message {0} when KafkaConsumerActor is in stopping stage", x));
        }

        protected override void PreStart()
        {
            base.PreStart();

            consumer = _settings.CreateKafkaConsumer();
            currentPollTask = SchedulePollTask();
        }

        protected override void PostStop()
        {
            currentPollTask?.Cancel();

            // reply to outstanding requests is important if the actor is restarted
            foreach (var entry in requests)
            {
                entry.Key.Tell(new Internal.Messages<TKey, TValue>(entry.Value.RequestId, Enumerable.Empty<Message<TKey, TValue>>()));
            }

            consumer.Dispose();
            base.PostStop();
        }

        private ICancelable SchedulePollTask() =>
            Context.System.Scheduler.ScheduleTellOnceCancelable(PollInterval, Self, pollMsg, ActorRefs.NoSender);

        protected void Poll()
        {
            //var wakeUpTask = Context.System.Scheduler.Advanced.ScheduleOnceCancelable(settings.WakeUpTimeout, () =>
            //{
            //    consumer.WakeUp();
            //});

            // set partitions to fetch
            var partitionsToFetch = requests.SelectMany(entry => entry.Value.Topics).ToImmutableHashSet();
            //foreach (var partition in consumer.Assignment)
            //{
            //    if (partitionsToFetch.Contains(partition))
            //    {
            //        consumer.Resume(partition);
            //    }
            //    else
            //    {
            //        consumer.Pause(partition);
            //    }
            //}

            if (requests.Count == 0)
            {
                // no outstanding requests so we don't expect any messages back, but we should anyway
                // drive the KafkaConsumer by polling
                CheckNoResult(TryPoll(TimeSpan.Zero));

                // For commits we try to avoid blocking poll because a commit normally succeeds after a few
                // poll(0). Using poll(1) will always block for 1 ms, since there are no messages.
                // Therefore we do 10 poll(0) with short 10 μs delay followed by 1 poll(1).
                // If it's still not completed it will be tried again after the scheduled Poll.
                for (int i = 10; i >= 0 && commitsInProgress > 0; i--)
                {
                    //Thread.SpinWait(10000);
                }
            }
            else
            {
                ProcessResult(partitionsToFetch, TryPoll(PollTimeout));
            }

            if (stopInProgress && commitsInProgress == 0)
            {
                Context.Stop(Self);
            }
        }

        private void CheckNoResult(Message<TKey, TValue> result)
        {
        }

        private Message<TKey, TValue> TryPoll(TimeSpan pollTimeout)
        {
            throw new NotImplementedException();
        }

        private void ProcessResult(ImmutableHashSet<TopicPartition> partitionsToFetch, Message<TKey, TValue> result)
        {
            throw new NotImplementedException();
        }

        private void CheckOverlappingRequests(string updateType, IActorRef fromStage, IImmutableSet<TopicPartition> topics)
        {
            // check if same topics/partitions have already been requested by someone else,
            // which is an indication that something is wrong, but it might be alright when assignments change.
            foreach (var entry in requests)
            {
                var reference = entry.Key;
                var request = entry.Value;
                if (!Equals(reference, fromStage) && !request.Topics.Intersect(topics).IsEmpty)
                {
                    if (Log.IsWarningEnabled)
                        Log.Warning("{0} from topic/partition [{1}] already requested by other stage [{2}]", updateType, string.Join(", ", topics), string.Join(", ", request.Topics));

                    reference.Tell(new Internal.Messages<TKey, TValue>(request.RequestId, Enumerable.Empty<Message<TKey, TValue>>()));
                    requests.Remove(reference);
                }
            }
        }
    }
}