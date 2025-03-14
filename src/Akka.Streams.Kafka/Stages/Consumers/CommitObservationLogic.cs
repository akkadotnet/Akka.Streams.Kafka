using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;

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
    public ICommittableOffsetBatch OffsetBatch { get; private set; } = CommittableOffsetBatch.Empty;
    
    /// <summary>
    /// Deferred offsets when <see cref="CommitterSettings.When"/>
    /// </summary>
    public ImmutableDictionary<GroupTopicPartition, ICommittable> DeferredOffsets { get; private set; } = ImmutableDictionary<GroupTopicPartition, ICommittable>.Empty;
}