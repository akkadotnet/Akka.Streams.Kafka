// -----------------------------------------------------------------------
//  <copyright file="CommitObservationLogic.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers;

/// <summary>
/// Shared commit observation logic for different graph stage logics that facilitate offset commits.
/// </summary>
/// <remarks>
/// Really should be implemented as an interface with default implementations, but we can't have nice
/// things in .NET Standard 2.0. Therefore this gets injected a dependency instead.
/// </remarks>
internal sealed class CommitObservationLogic
{
    public CommitObservationLogic(CommitterSettings settings)
    {
        Settings = settings;
    }

    public CommitterSettings Settings { get; }

    /// <summary>
    /// Batches offsets until a commit is triggered
    /// </summary>
    public ICommittableOffsetBatch OffsetBatch { get; set; } = CommittableOffsetBatch.Empty;

    /// <summary>
    /// Deferred offsets when <see cref="CommitterSettings.When"/>
    /// </summary>
    public ImmutableDictionary<GroupTopicPartition, ICommittable> DeferredOffsets { get; private set; } =
        ImmutableDictionary<GroupTopicPartition, ICommittable>.Empty;

    /// <summary>
    /// Update the offset batch when applicable given the <see cref="ICommitWhen"/> settings.
    /// </summary>
    /// <returns><c>true</c> if the batch is ready to be committed.</returns>
    public bool UpdateBatch(ICommittable committable)
    {
        if (Settings.When == CommitWhen.OffsetFirstObserved.Instance)
        {
            OffsetBatch = OffsetBatch.Updated(committable);
        }
        else // CommitWhen.NextOffsetObserved
        {
            switch (committable)
            {
                case CommittableOffset single:
                {
                    var gtp = single.Offset.GroupTopicPartition;
                    UpdateBatchForPartition(gtp, single, single.Offset.Offset);
                    break;
                }

                case CommittableOffsetBatch batch:
                {
                    foreach (var (gtp, offsetAndMetadata) in batch.OffsetsAndMetadata)
                    {
                        UpdateBatchForPartition(gtp,
                            batch.Filter(c => c.Equals(gtp)),
                            offsetAndMetadata.Offset);
                    }

                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(committable),
                        "Unknown committable type, expected CommittableOffset or CommittableOffsetBatch, got " +
                        committable.GetType().FullName);
            }
        }

        return OffsetBatch.BatchSize >= Settings.MaxBatch;
    }

    private void UpdateBatchForPartition(GroupTopicPartition groupTopicPartition, ICommittable committable,
        Offset offset)
    {
        if (DeferredOffsets.TryGetValue(groupTopicPartition, out var deferredOffsets))
        {
            switch (deferredOffsets)
            {
                case CommittableOffset dOffset when dOffset.Offset.Offset < offset:
                    // Higher offset for this partition, update the deferred offset
                    DeferredOffsets = DeferredOffsets.SetItem(groupTopicPartition, committable);
                    OffsetBatch = OffsetBatch.Updated(dOffset);
                    break;
                case CommittableOffsetBatch dOffsetBatch
                    when dOffsetBatch.Offsets.ContainsKey(groupTopicPartition)
                         && dOffsetBatch.Offsets[groupTopicPartition].Value < offset:
                    DeferredOffsets = DeferredOffsets.SetItem(groupTopicPartition, committable);
                    OffsetBatch = OffsetBatch.Updated(dOffsetBatch);
                    break;
            }
        }
        else
        {
            DeferredOffsets = DeferredOffsets.SetItem(groupTopicPartition, committable);
        }
    }

    /// <summary>
    /// Clear any deferred offsets.
    /// </summary>
    /// <remarks>
    /// This should only be called once when a committing stage is shutting down.
    /// </remarks>
    /// <returns>The size of the deferred offsets</returns>
    public int ClearDeferredOffsets()
    {
        var size = DeferredOffsets.Count;
        DeferredOffsets = ImmutableDictionary<GroupTopicPartition, ICommittable>.Empty;
        return size;
    }
}