using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
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
                                      IImmutableDictionary<string, IInternalCommitter> committers, 
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
        
        /// <summary>
        /// Committers
        /// </summary>
        public IImmutableDictionary<string, IInternalCommitter> Committers { get; }
        
        /// <summary>
        /// Offsets and metadata
        /// </summary>
        public IImmutableDictionary<GroupTopicPartition, OffsetAndMetadata> OffsetsAndMetadata { get; }
        
        /// <summary>
        /// Create empty offset batch
        /// </summary>
        public static ICommittableOffsetBatch Empty => new CommittableOffsetBatch(ImmutableDictionary<GroupTopicPartition, OffsetAndMetadata>.Empty, 
                                                                                  ImmutableDictionary<string, IInternalCommitter>.Empty, 
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

        /// <inheritdoc />
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
            if (!(committableOffsetBatch is CommittableOffsetBatch committableOffsetBatchImpl))
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
            
            
            IImmutableDictionary<string, IInternalCommitter> newCommitters = ImmutableDictionary<string, IInternalCommitter>.Empty;
            if (Committers.TryGetValue(partitionOffset.GroupId, out var groupCommitter))
            {
                if (!groupCommitter.Equals(committer))
                {
                    throw new ArgumentException($"CommittableOffset {committableOffset} committer for groupId {partitionOffset.GroupId} " +
                                                $"must be same as the other with this groupId.");
                }

                newCommitters = Committers;
            }
            else
            {
                newCommitters = Committers.SetItem(partitionOffset.GroupId, committer);
            }
            
            return new CommittableOffsetBatch(newOffsets, newCommitters, BatchSize + 1);
        }
    }
}