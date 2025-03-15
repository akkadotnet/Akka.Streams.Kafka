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
                                      IImmutableDictionary<GroupTopicPartition, KafkaAsyncConsumerCommitter> committers, 
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
        public IImmutableDictionary<GroupTopicPartition, KafkaAsyncConsumerCommitter> Committers { get; }
        
        /// <summary>
        /// Offsets and metadata
        /// </summary>
        public IImmutableDictionary<GroupTopicPartition, OffsetAndMetadata> OffsetsAndMetadata { get; }
        
        /// <summary>
        /// Create empty offset batch
        /// </summary>
        public static ICommittableOffsetBatch Empty => new CommittableOffsetBatch(ImmutableDictionary<GroupTopicPartition, OffsetAndMetadata>.Empty, 
                                                                                  ImmutableDictionary<GroupTopicPartition, KafkaAsyncConsumerCommitter>.Empty, 
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
        
        public Task Commit()
        {
            return KafkaAsyncConsumerCommitter.Commit(this);
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
            switch (committableOffsetBatch)
            {
                case CommittableOffsetBatch newBatch:
                {
                    var newOffsetsAndMetadata = OffsetsAndMetadata.AddRange(newBatch.OffsetsAndMetadata);
                    var newCommitters = Committers.AddRange(newBatch.Committers);
                    break;
                }
            }
        }

        /// <summary>
        /// Adds committable offset to existing ones
        /// </summary>
        private ICommittableOffsetBatch UpdateWithOffset(ICommittableOffset newOffset)
        {
            var partitionOffset = newOffset.Offset;
            var key = partitionOffset.GroupTopicPartition;
            var metadata = (newOffset is ICommittableOffsetMetadata withMetadata) ? withMetadata.Metadata : string.Empty;

            var newOffsets = OffsetsAndMetadata.SetItem(key, new OffsetAndMetadata(partitionOffset.Offset, metadata));

            var newCommitter = newOffset switch
            {
                CommittableOffset c => c.Committer,
                _ => throw new ArgumentException(
                    $"Unknown committable offset, got {newOffset.GetType().Name}, expected {nameof(CommittableOffset)}")
            };
            
            // the last KafkaAsyncConsumerCommitter for this GroupTopicPartition wins
            var newCommitters = Committers.SetItem(key, newCommitter);
            
            return new CommittableOffsetBatch(newOffsets, newCommitters, BatchSize + 1);
        }
        
        internal KafkaAsyncConsumerCommitter CommitterFor(GroupTopicPartition groupTopicPartition)
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