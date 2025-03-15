using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Stages.Consumers;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Stores committable offsets batch and allows to commit them with <see cref="Commit"/> method
    /// </summary>
    internal sealed class CommittableOffsetBatch : ICommittableOffsetBatch
    {
        /// <summary>
        /// CommittableOffsetBatch
        /// </summary>
        public CommittableOffsetBatch(IImmutableDictionary<GroupTopicPartition, OffsetAndMetadata> offsetsAndMetadata, 
                                      IImmutableDictionary<GroupTopicPartition, IInternalCommitter> committers, 
                                      long batchSize)
        {
            OffsetsAndMetadata = offsetsAndMetadata;
            Committers = committers;
            BatchSize = batchSize;
        }

        /// <inheritdoc />
        public long BatchSize { get; }
        
        /// <inheritdoc />
        public IImmutableSet<GroupTopicPartitionOffset> Offsets => OffsetsAndMetadata.Select(o => new GroupTopicPartitionOffset(o.Key, o.Value.Offset)).ToImmutableHashSet();

        public bool IsEmpty => BatchSize == 0;
        public void TellCommitEmergency() => throw new NotImplementedException();

        /// <summary>
        /// Committers
        /// </summary>
        public IImmutableDictionary<GroupTopicPartition, IInternalCommitter> Committers { get; }
        
        /// <summary>
        /// Offsets and metadata
        /// </summary>
        public IImmutableDictionary<GroupTopicPartition, OffsetAndMetadata> OffsetsAndMetadata { get; }
        
        /// <summary>
        /// Create empty offset batch
        /// </summary>
        public static ICommittableOffsetBatch Empty => new CommittableOffsetBatch(ImmutableDictionary<GroupTopicPartition, OffsetAndMetadata>.Empty, 
                                                                                  ImmutableDictionary<GroupTopicPartition, IInternalCommitter>.Empty, 
                                                                                  0);
        
        /// <summary>
        /// Create an offset batch out of a first offsets.
        /// </summary>
        public static ICommittableOffsetBatch Create(ICommittableOffset offset) => Empty.Updated(offset);
        
        /// <summary>
        /// Create an offset batch out of a list of offsets.
        /// </summary>
        public static ICommittableOffsetBatch Create(IEnumerable<ICommittable> offsets)
        {
            return offsets.Aggregate(Empty, (batch, offset) => batch.Updated(offset));
        }

        /// <inheritdoc />
        public async Task Commit()
        {
            if (Offsets.IsEmpty() || Committers.IsEmpty())
                return;

            await Committers.First().Value.Commit(this);
        }
        
        public ICommittableOffsetBatch Updated(ICommittable offset)
        {
            switch (offset)
            {
                case ICommittableOffset committableOffset:
                    return UpdateWithOffset(committableOffset);
                case ICommittableOffsetBatch committableOffsetBatch:
                    return UpdateWithBatch(committableOffsetBatch);
                default:
                    throw new AggregateException($"Unexpected offset to update committable batch offsets from: {offset.GetType().Name}");
            }
        }

        /// <summary>
        /// Adds offsets from given committable batch to existing ones
        /// </summary>
        private ICommittableOffsetBatch UpdateWithBatch(ICommittableOffsetBatch committableOffsetBatch)
        {
            if (committableOffsetBatch is not CommittableOffsetBatch committableOffsetBatchImpl)
                throw new ArgumentException($"Unexpected CommittableOffsetBatch, got {committableOffsetBatch.GetType().Name}, expected {nameof(CommittableOffsetBatch)}");

            var newOffsetsAndMetdata = OffsetsAndMetadata.SetItems(committableOffsetBatchImpl.OffsetsAndMetadata);
            var newCommitters = committableOffsetBatchImpl.Committers.Aggregate(Committers, (committers, pair) =>
            {
                var groupId = pair.Key;
                var committer = pair.Value;
                if (committers.TryGetValue(groupId, out var groupCommitter))
                {
                    if (!groupCommitter.Equals(committer))
                    {
                        throw new ArgumentException($"CommittableOffsetBatch {committableOffsetBatch} committer for groupId {groupId} " +
                                                    $"must be same as the other with this groupId.");
                    }

                    return committers;
                }
                else
                {
                    return committers.Add(groupId, committer);
                }
            }).ToImmutableDictionary(pair => pair.Key, pair => pair.Value);
            
            return new CommittableOffsetBatch(newOffsetsAndMetdata, newCommitters, BatchSize + committableOffsetBatchImpl.BatchSize);
        }

        /// <summary>
        /// Adds committable offset to existing ones
        /// </summary>
        private ICommittableOffsetBatch UpdateWithOffset(ICommittableOffset committableOffset)
        {
            var partitionOffset = committableOffset.Offset;
            var metadata = (committableOffset is ICommittableOffsetMetadata withMetadata) ? withMetadata.Metadata : string.Empty;

            var newOffsets = OffsetsAndMetadata.SetItem(partitionOffset.GroupTopicPartition, new OffsetAndMetadata(partitionOffset.Offset, metadata));
            var committer = committableOffset is CommittableOffset c 
                ? c.Committer 
                : throw new ArgumentException($"Unknown committable offset, got {committableOffset.GetType().Name}, expected {nameof(committableOffset)}");
            
            
            IImmutableDictionary<GroupTopicPartition, IInternalCommitter> newCommitters;
            if (Committers.TryGetValue(partitionOffset.GroupTopicPartition, out var groupCommitter))
            {
                if (!groupCommitter.Equals(committer))
                {
                    throw new ArgumentException($"CommittableOffset {committableOffset} committer for groupId {partitionOffset.GroupTopicPartition} " +
                                                $"must be same as the other with this groupId.");
                }

                newCommitters = Committers;
            }
            else
            {
                newCommitters = Committers.SetItem(partitionOffset.GroupTopicPartition, committer);
            }
            
            return new CommittableOffsetBatch(newOffsets, newCommitters, BatchSize + 1);
        }
        
        internal IInternalCommitter CommitterFor(GroupTopicPartition groupTopicPartition)
        {
            if (Committers.TryGetValue(groupTopicPartition, out var committer))
            {
                return committer;
            }
            throw new ArgumentException($"Unknown committer for groupId {groupTopicPartition}");
        }

        internal ICommittableOffsetBatch Filter(Predicate<GroupTopicPartition> p)
        {
            var newOffsets = OffsetsAndMetadata.Where(o => p(o.Key))
                .ToImmutableDictionary(o => o.Key, o => o.Value);
            var newCommiters =
                Offsets.ToImmutableDictionary(c => c.GroupTopicPartition, v => CommitterFor(v.GroupTopicPartition));
            return new CommittableOffsetBatch(newOffsets, newCommiters, BatchSize);
        }

        public override string ToString() => $"CommittableOffsetBatch(BatchSize={BatchSize}, {string.Join(",", Offsets)})";
    }
}