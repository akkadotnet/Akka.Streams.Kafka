using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Streams.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    internal class TransactionalSourceLogic<K, V> : SingleSourceStageLogic<K, V, TransactionalMessage<K, V>>, 
                                                             ITransactionalMessageBuilderStage<K, V, TransactionalMessage<K, V>>
    {
        private readonly SourceShape<TransactionalMessage<K, V>> _shape;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly TransactionalMessageBuilder<K, V> _messageBuilder;
        private readonly InFlightRecords _inFlightRecords = InFlightRecords.Empty;
        private readonly Lazy<ICommittedMarker> _committedMarker;
        
        public TransactionalSourceLogic(
                SourceShape<TransactionalMessage<K, V>> shape, 
                ConsumerSettings<K, V> settings, 
                ISubscription subscription, 
                Attributes attributes,
                Func<ITransactionalMessageBuilderStage<K, V, TransactionalMessage<K, V>>, TransactionalMessageBuilder<K, V>> messageBuilderFactory) 
            : base(shape, settings, subscription, attributes, logic => messageBuilderFactory(logic as ITransactionalMessageBuilderStage<K, V, TransactionalMessage<K, V>>))
        {
            _shape = shape;
            _settings = settings;
            _subscription = subscription;
            _messageBuilder = messageBuilderFactory(this);
            
            _committedMarker = new Lazy<ICommittedMarker>(() => new CommittedMarkerRef(SourceActor.Ref, _settings.CommitTimeout));
        }

        /// <inheritdoc />
        protected override void MessageHandling(Tuple<IActorRef, object> args)
        {
            DrainHandling(args, notHandledArgs =>
            {
                switch (notHandledArgs.Item2)
                {
                    case KafkaConsumerActorMetadata.Internal.Revoked revoked:
                        _inFlightRecords.Revoke(revoked.Partitions);
                        break;
                    default:
                        base.MessageHandling(notHandledArgs);
                        break;
                }
            });
        }

        /// <inheritdoc />
        protected override void ShuttingDownReceive(Tuple<IActorRef, object> args)
        {
            DrainHandling(args, notHandledArgs =>
            {
                switch (notHandledArgs.Item2)
                {
                    case Status.Failure failure:
                        FailStage(failure.Cause);
                        break;
                        
                    case Terminated terminated when terminated.ActorRef.Equals(ConsumerActor):
                        FailStage(new ConsumerFailed());
                        break;
                    
                    default:
                        base.ShuttingDownReceive(notHandledArgs);
                        break;
                }
            });
        }

        private void DrainHandling(Tuple<IActorRef, object> arg, Action<Tuple<IActorRef, object>> defaultHandler)
        {
            var (sender, msg) = arg;
            switch (msg)
            {
                case KafkaConsumerActorMetadata.Internal.Committed committed:
                    _inFlightRecords.Committed(committed.Offsets.ToImmutableDictionary(offset => offset.TopicPartition, offset => new Offset(offset.Offset - 1)));
                    sender.Tell(Done.Instance);
                    break;
                
                case CommittingFailure committingFailure:
                    Log.Info("Committing failed, resetting in flight offsets");
                    _inFlightRecords.Reset();
                    break;
                    
                case Drain drain:
                    if (_inFlightRecords.IsEmpty(drain.Partitions))
                    {
                        Log.Debug($"Partitions drained {drain.Partitions.JoinToString(", ")}");
                        drain.DrainedConfirmationRef.GetOrElse(sender).Tell(drain.DrainedConfirmationMessage);
                    }
                    else
                    {
                        Log.Debug("Draining partitions {0}", drain.Partitions);
                        Materializer.ScheduleOnce(_settings.DrainingCheckInterval, () =>
                        {
                           SourceActor.Ref.Tell(new Drain(drain.Partitions, drain.DrainedConfirmationRef.GetOrElse(sender).AsOption(), drain.DrainedConfirmationMessage)); 
                        });
                    }
                    break;
                
                default:
                    defaultHandler(arg);
                    break;
            }
        }

        /// <inheritdoc />
        public TransactionalMessage<K, V> CreateMessage(ConsumeResult<K, V> record) => _messageBuilder.CreateMessage(record);

        /// <inheritdoc />
        public string GroupId => _settings.GroupId;

        /// <inheritdoc />
        public ICommittedMarker CommittedMarker => _committedMarker.Value;

        /// <inheritdoc />
        public void OnMessage(ConsumeResult<K, V> message)
        {
            _inFlightRecords.Add(ImmutableDictionary.Create<TopicPartition, Offset>().Add(message.TopicPartition, message.Offset));
        }

        /// <inheritdoc />
        protected override void StopConsumerActor()
        {
            SourceActor.Ref.Tell(new Drain(_inFlightRecords.Assigned, ConsumerActor.AsOption(), new KafkaConsumerActorMetadata.Internal.Stop()), SourceActor.Ref);
        }

        /// <inheritdoc />
        protected override IPartitionEventHandler<K, V> AddToPartitionAssignmentHandler(IPartitionEventHandler<K, V> handler)
        {
            var blockingRevokedCall = new AsyncCallbacksPartitionEventHandler<K, V>(
                partitionAssignedCallback: _ => { },
                partitionRevokedCallback: revokedTopicPartitions =>
                {
                    var topicPartitions = revokedTopicPartitions.Select(tp => tp.TopicPartition).ToImmutableHashSet();
                    if (WaitForDraining(topicPartitions))
                    {
                        SourceActor.Ref.Tell(new KafkaConsumerActorMetadata.Internal.Revoked(topicPartitions));
                    }
                    else
                    {
                        SourceActor.Ref.Tell(new Status.Failure(new Exception("Timeout while drailing")));
                        ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Stop());
                    }
                });
            
            return new PartitionAssignedHandlersChain<K, V>(handler, blockingRevokedCall);
        }

        private bool WaitForDraining(IImmutableSet<TopicPartition> partitions)
        {
            try
            {
                StageActor.Ref.Ask(new Drain(partitions, Option<IActorRef>.None, new Drained()), _settings.CommitTimeout);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Message class for committing failure indication
        /// </summary>
        private sealed class CommittingFailure { }
        
        /// <summary>
        /// Message for check drailnng state
        /// </summary>
        private sealed class Drained { }

        /// <summary>
        /// Drain
        /// </summary>
        /// <typeparam name="T">Drain message type</typeparam>
        private sealed class Drain
        {
            public Drain(IImmutableSet<TopicPartition> partitions, Option<IActorRef> drainedConfirmationRef, object drainedConfirmationMessage)
            {
                Partitions = partitions;
                DrainedConfirmationRef = drainedConfirmationRef;
                DrainedConfirmationMessage = drainedConfirmationMessage;
            }

            /// <summary>
            /// Drained partitions
            /// </summary>
            public IImmutableSet<TopicPartition> Partitions { get; }
            /// <summary>
            /// Drained confirmation actor ref
            /// </summary>
            public Option<IActorRef> DrainedConfirmationRef { get; }
            /// <summary>
            /// Drained confirmation message
            /// </summary>
            public object DrainedConfirmationMessage { get; }
        }

        /// <summary>
        /// Used for marking committed offsets at <see cref="TransactionalSourceLogic{K,V}"/>
        /// </summary>
        private sealed class CommittedMarkerRef : ICommittedMarker
        {
            private readonly IActorRef _sourceActor;
            private readonly TimeSpan _commitTimeout;

            public CommittedMarkerRef(IActorRef sourceActor, TimeSpan commitTimeout)
            {
                _sourceActor = sourceActor;
                _commitTimeout = commitTimeout;
            }

            /// <inheritdoc />
            public async Task Committed(IImmutableDictionary<TopicPartition, OffsetAndMetadata> offsets)
            {
                var topicPartitionOffsets = offsets.Select(o => new TopicPartitionOffset(o.Key, o.Value.Offset)).ToImmutableHashSet();
                await _sourceActor.Ask(new KafkaConsumerActorMetadata.Internal.Committed(topicPartitionOffsets), _commitTimeout);
            }

            /// <inheritdoc />
            public void Failed() => _sourceActor.Tell(new CommittingFailure());
        }
        
        /// <summary>
        /// InFlightRecords
        /// </summary>
        private class InFlightRecords
        {
            private IImmutableDictionary<TopicPartition, Offset> _inFlightRecords = ImmutableDictionary<TopicPartition, Offset>.Empty;

            private InFlightRecords()
            {
            }
            
            /// <summary>
            /// Gets empty collection
            /// </summary>
            public static InFlightRecords Empty => new InFlightRecords();
            
            /// <summary>
            /// Assumes that offsets per topic partition are added in the increasing order
            /// The assumption is true for Kafka consumer that guarantees that elements are emitted
            /// per partition in offset-increasing order.
            /// </summary>
            public void Add(IImmutableDictionary<TopicPartition, Offset> offsets) => _inFlightRecords = _inFlightRecords.AddRange(offsets);

            /// <summary>
            /// Removes committed records from collection
            /// </summary>
            public void Committed(IImmutableDictionary<TopicPartition, Offset> committed)
            {
                _inFlightRecords = _inFlightRecords.Where(pair => !committed.ContainsKey(pair.Key) || committed[pair.Key] != pair.Value).ToImmutableDictionary();
            }
            
            /// <summary>
            /// Removes revoked partitions from collection
            /// </summary>
            public void Revoke(IEnumerable<TopicPartition> topicPartitions) => _inFlightRecords = _inFlightRecords.RemoveRange(topicPartitions);

            /// <summary>
            /// Reset collection
            /// </summary>
            public void Reset() => _inFlightRecords = ImmutableDictionary<TopicPartition, Offset>.Empty;
            
            /// <summary>
            /// Checks if specified partition in flight records collection is empty 
            /// </summary>
            public bool IsEmpty(IImmutableSet<TopicPartition> partitions) => !partitions.Any(tp => _inFlightRecords.ContainsKey(tp));

            /// <summary>
            /// Gets list of assigned topic partitions
            /// </summary>
            public IImmutableSet<TopicPartition> Assigned => _inFlightRecords.Keys.ToImmutableHashSet();

            /// <inheritdoc />
            public override string ToString() => _inFlightRecords.ToString();
        }
    }
}