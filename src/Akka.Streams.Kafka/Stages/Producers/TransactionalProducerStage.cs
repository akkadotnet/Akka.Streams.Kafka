using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    /// <summary>
    /// Stage used by <see cref="KafkaProducer.TransactionalFlow{K,V}"/>
    /// </summary>
    internal class TransactionalProducerStage<K, V, TPassThrough> : GraphStage<FlowShape<IEnvelope<K, V, TPassThrough>, Task<IResults<K, V, TPassThrough>>>>,
                                                                    IProducerStage<K, V, TPassThrough, IEnvelope<K, V, TPassThrough>, IResults<K, V, TPassThrough>>
    {
        private readonly ProducerSettings<K, V> _settings;
        
        public TimeSpan FlushTimeout => _settings.FlushTimeout;
        public bool CloseProducerOnStop { get; }
        public Func<Action<IProducer<K, V>, Error>, IProducer<K, V>> ProducerProvider { get; }
        public override FlowShape<IEnvelope<K, V, TPassThrough>, Task<IResults<K, V, TPassThrough>>> Shape { get; }
        public Inlet<IEnvelope<K, V, TPassThrough>> In { get; } = new Inlet<IEnvelope<K, V, TPassThrough>>("kafka.transactional.producer.in");
        public Outlet<Task<IResults<K, V, TPassThrough>>> Out { get; } = new Outlet<Task<IResults<K, V, TPassThrough>>>("kafka.transactional.producer.out");

        /// <summary>
        /// TransactionalProducerStage
        /// </summary>
        public TransactionalProducerStage(bool closeProducerOnStop, ProducerSettings<K, V> settings)
        {
            _settings = settings;
            
            CloseProducerOnStop = closeProducerOnStop;
            ProducerProvider = errorHandler => _settings.CreateKafkaProducer(errorHandler);
            Shape = new FlowShape<IEnvelope<K, V, TPassThrough>, Task<IResults<K, V, TPassThrough>>>(In, Out);
        }

        /// <inheritdoc />
        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return new TransactionalProducerStageLogic<K, V, TPassThrough>(this, inheritedAttributes, _settings.EosCommitInterval);
        }
    }
    
    internal class TransactionalProducerStageLogic<K, V, TPassThrough> : DefaultProducerStageLogic<K, V, TPassThrough, IEnvelope<K, V, TPassThrough>, IResults<K, V, TPassThrough>>
    {
        private const string CommitSchedulerKey = "commit";
        
        private readonly TransactionalProducerStage<K, V, TPassThrough> _stage;
        private readonly TimeSpan _commitInterval;
        private readonly Decider _decider;
        
        private readonly TimeSpan _messageDrainInterval = TimeSpan.FromMilliseconds(10);
        private ITransactionBatch _batchOffsets = new EmptyTransactionBatch();
        private bool _demandSuspended = false;
        private readonly Action _onInternalCommitCallback;
        
        public TransactionalProducerStageLogic(
                TransactionalProducerStage<K, V, TPassThrough> stage, 
                Attributes attributes,
                TimeSpan commitInterval) 
            : base(stage, attributes)
        {
            _stage = stage;
            _commitInterval = commitInterval;

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.StoppingDecider;

            _onInternalCommitCallback = GetAsyncCallback(() => ScheduleOnce(CommitSchedulerKey, commitInterval));
        }

        public override void PreStart()
        {
            base.PreStart();

            InitTransactions();
            BeginTransaction();
            ResumeDemand(tryToPull: false);
            ScheduleOnce(CommitSchedulerKey, _commitInterval);
        }

        private void ResumeDemand(bool tryToPull = true)
        {
            SetHandler(_stage.Out, onPull: () => TryPull(_stage.In));

            _demandSuspended = false;

            if (tryToPull && IsAvailable(_stage.Out) && !HasBeenPulled(_stage.In))
            {
                TryPull(_stage.In);
            }
        }

        private void SuspendDemand()
        {
            if (!_demandSuspended)
            {
                // suspend demand while a commit is in process so we can drain any outstanding message acknowledgements
                SetHandler(_stage.Out, onPull: () => { });
            }

            _demandSuspended = true;
        }

        protected override void OnTimer(object timerKey)
        {
            if (timerKey.Equals(CommitSchedulerKey))
                MaybeCommitTransaction();
        }

        private void MaybeCommitTransaction(bool beginNewTransaction = true)
        {
            var awaitingConf = AwaitingConfirmation.Current;
            if (_batchOffsets is NonemptyTransactionBatch nonemptyTransactionBatch && awaitingConf == 0)
            {
                CommitTransaction(nonemptyTransactionBatch, beginNewTransaction);
            }
            else if (awaitingConf > 0)
            {
                SuspendDemand();
                ScheduleOnce(CommitSchedulerKey, _messageDrainInterval);
            }
            else
            {
                ScheduleOnce(CommitSchedulerKey, _commitInterval);
            }
        }

        protected override void PostSend(IEnvelope<K, V, TPassThrough> msg)
        {
            var marker = msg.PassThrough as PartitionOffsetCommittedMarker;
            if (marker != null)
                _batchOffsets = _batchOffsets.Updated(marker);
        }

        public override void OnCompletionSuccess()
        {
            Log.Debug("Comitting final transaction before shutdown");
            CancelTimer(CommitSchedulerKey);
            MaybeCommitTransaction(beginNewTransaction: false);
            base.OnCompletionSuccess();
        }

        public override void OnCompletionFailure(Exception ex)
        {
            Log.Debug("Aborting transaction due to stage failure");
            AbortTransaction();
            _batchOffsets.CommittingFailed();
            base.OnCompletionFailure(ex);
        }

        private void CommitTransaction(NonemptyTransactionBatch batch, bool beginNewTransaction)
        {
            var groupId = batch.GroupId;
            Log.Debug("Committing transaction for consumer group '{0}' with offsets: {1}", groupId, batch.Offsets);
            var offsetMap = batch.OffsetMap();
            
            // TODO: Add producer work with transactions
            /* scala code:
             producer.sendOffsetsToTransaction(offsetMap.asJava, group)
             producer.commitTransaction()
             */
            Log.Debug("Committed transaction for consumer group '{0}' with offsets: {1}", groupId, batch.Offsets);
            _batchOffsets = new EmptyTransactionBatch();
            batch.InternalCommit().ContinueWith(t =>
            {
                _onInternalCommitCallback.Invoke();
            });

            if (beginNewTransaction)
            {
                BeginTransaction();
                ResumeDemand();
            }
        }

        private void InitTransactions()
        {
            Log.Debug("Iinitializing transactions");
            // TODO: Add producer work with transactions
            // producer.initTransactions()
        }

        private void BeginTransaction()
        {
            Log.Debug("Beginning new transaction");
            // TODO: Add producer work with transactions
            // producer.beginTransaction()
        }

        private void AbortTransaction()
        {
            Log.Debug("Aborting transaction");
            // TODO: Add producer work with transactions
            // producer.abortTransaction()
        }
        

        private interface ITransactionBatch
        {
            ITransactionBatch Updated(PartitionOffsetCommittedMarker partitionOffset);
            void CommittingFailed();
        }

        private class EmptyTransactionBatch : ITransactionBatch
        {
            public ITransactionBatch Updated(PartitionOffsetCommittedMarker partitionOffset) => new NonemptyTransactionBatch(partitionOffset);

            public void CommittingFailed()
            {
            }
        }

        private class NonemptyTransactionBatch : ITransactionBatch
        {
            private readonly PartitionOffsetCommittedMarker _head;
            private readonly IImmutableDictionary<GroupTopicPartition, Offset> _tail;

            private readonly ICommittedMarker _committedMarker;
            
            public IImmutableDictionary<GroupTopicPartition, Offset> Offsets { get; }
            public string GroupId { get; }

            public NonemptyTransactionBatch(PartitionOffsetCommittedMarker head, IImmutableDictionary<GroupTopicPartition, Offset> tail = null)
            {
                _head = head;
                _tail = tail ?? ImmutableDictionary<GroupTopicPartition, Offset>.Empty;

                _committedMarker = head.CommittedMarker;
                GroupId = head.GroupId;

                var previousHighest = _tail.GetValueOrDefault(head.GroupTopicPartition, new Offset(-1)).Value;
                var highestOffset = new Offset(Math.Max(head.Offset, previousHighest));
                Offsets = _tail.AddRange(new []{ new KeyValuePair<GroupTopicPartition, Offset>(head.GroupTopicPartition, highestOffset) });
            }
            
            /// <inheritdoc />
            public ITransactionBatch Updated(PartitionOffsetCommittedMarker partitionOffset)
            {
                if (partitionOffset.GroupId != GroupId)
                    throw new ArgumentException($"Transaction batch must contain messages from exactly 1 consumer group. {partitionOffset.GroupId} != {GroupId}");
                
                if (!partitionOffset.CommittedMarker.Equals(_committedMarker))
                    throw new ArgumentException("Transaction batch must contain messages from a single source");
                
                return new NonemptyTransactionBatch(partitionOffset, Offsets);
            }

            /// <inheritdoc />
            public void CommittingFailed() => _committedMarker.Failed();

            public IImmutableDictionary<TopicPartition, OffsetAndMetadata> OffsetMap()
            {
                return Offsets.ToImmutableDictionary(
                    pair => new TopicPartition(pair.Key.Topic, pair.Key.Partition),
                    pair => new OffsetAndMetadata(pair.Value.Value + 1, string.Empty)
                );
            }

            public Task InternalCommit() => _committedMarker.Committed(OffsetMap());
        }
    }
}