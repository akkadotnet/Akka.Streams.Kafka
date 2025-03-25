// -----------------------------------------------------------------------
//  <copyright file="CommittableOffsetBatch.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Stages.Consumers;

namespace Akka.Streams.Kafka.Messages;

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
    
    public long BatchSize { get; }

    /// Represents the offsets as they are, rather than how they're going to be committed.
    ///
    /// We have to +1 all offsets upon commit - which is what you get inside OffsetsAndMetadata.
    ///
    /// This is useful for debugging and for testing, but not much else.
    public IImmutableSet<GroupTopicPartitionOffset> Offsets
    {
        get
        {
            return OffsetsAndMetadata.Select(o => new GroupTopicPartitionOffset(o.Key, o.Value.Offset - 1L))
                .ToImmutableHashSet();
        }
    }

    public bool IsEmpty
    {
        get { return BatchSize == 0; }
    }

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
    public static ICommittableOffsetBatch Empty
    {
        get
        {
            return new CommittableOffsetBatch(ImmutableDictionary<GroupTopicPartition, OffsetAndMetadata>.Empty,
                ImmutableDictionary<GroupTopicPartition, KafkaAsyncConsumerCommitter>.Empty,
                0);
        }
    }

    /// <summary>
    /// Create an offset batch out of a first offsets.
    /// </summary>
    public static ICommittableOffsetBatch Create(ICommittableOffset offset) => Empty.Updated(offset);

    /// <summary>
    /// Create an offset batch out of a list of offsets.
    /// </summary>
    public static ICommittableOffsetBatch Create(IEnumerable<ICommittable> offsets) =>
        offsets.Aggregate(Empty, (batch, offset) => batch.Updated(offset));

    public Task Commit() => KafkaAsyncConsumerCommitter.Commit(this);

    public ICommittableOffsetBatch Updated(ICommittable offset)
    {
        switch (offset)
        {
            case ICommittableOffset committableOffset:
                return UpdateWithOffset(committableOffset);
            case ICommittableOffsetBatch committableOffsetBatch:
                return UpdateWithBatch(committableOffsetBatch);
            default:
                throw new AggregateException(
                    $"Unexpected offset to update committable batch offsets from: {offset.GetType().Name}");
        }
    }

    /// <summary>
    /// Adds offsets from given committable batch to existing ones
    /// </summary>
    private CommittableOffsetBatch UpdateWithBatch(ICommittableOffsetBatch committableOffsetBatch)
    {
        switch (committableOffsetBatch)
        {
            case CommittableOffsetBatch newBatch:
            {
                var newOffsetsAndMetadata = OffsetsAndMetadata.SetItems(newBatch.OffsetsAndMetadata);

                // the last KafkaAsyncConsumerCommitter for each GroupTopicPartition wins
                var newCommitters = Committers.SetItems(newBatch.Committers);
                return new CommittableOffsetBatch(newOffsetsAndMetadata, newCommitters, BatchSize + newBatch.BatchSize);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(committableOffsetBatch),
                    $"Unknown committable offset batch, got {committableOffsetBatch.GetType().Name}, expected {nameof(CommittableOffsetBatch)}");
        }
    }

    /// <summary>
    /// Adds committable offset to existing ones
    /// </summary>
    private CommittableOffsetBatch UpdateWithOffset(ICommittableOffset newOffset)
    {
        var partitionOffset = newOffset.Offset;
        var key = partitionOffset.GroupTopicPartition;
        var metadata = newOffset is ICommittableOffsetMetadata withMetadata ? withMetadata.Metadata : string.Empty;

        /*
         * https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
         * "The committed offset should always be the offset of the next message that your application will read.
         * Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
         */
        var newOffsets = OffsetsAndMetadata.SetItem(key, new OffsetAndMetadata(partitionOffset.Offset + 1, metadata));

        var newCommitter = newOffset switch
        {
            CommittableOffset c => c.Committer,
            _ => throw new ArgumentOutOfRangeException(nameof(newOffset),
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

    internal CommittableOffsetBatch Filter(Predicate<GroupTopicPartition> p)
    {
        var newOffsets = OffsetsAndMetadata.Where(o => p(o.Key))
            .ToImmutableDictionary(o => o.Key, o => o.Value);
        var newCommiters =
            Offsets.ToImmutableDictionary(c => c.GroupTopicPartition, v => CommitterFor(v.GroupTopicPartition));
        return new CommittableOffsetBatch(newOffsets, newCommiters, BatchSize);
    }

    public CommittableOffsetBatch TellCommit() => TellCommitWithPriority(false);

    public CommittableOffsetBatch TellCommitEmergency() => TellCommitWithPriority(true);

    private CommittableOffsetBatch TellCommitWithPriority(bool emergency)
    {
        KafkaAsyncConsumerCommitter.TellCommit(this, emergency);
        return this;
    }

    public override string ToString() => $"CommittableOffsetBatch(BatchSize={BatchSize}, {string.Join(",", Offsets)})";
}