// -----------------------------------------------------------------------
//  <copyright file="Committable.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Akka.Streams.Kafka.Messages;

/// <summary>
/// Commit an offset that is included in a <see cref="CommittableMessage{K,V}"/>
/// If you need to store offsets in anything other than Kafka, this API
/// should not be used.
/// </summary>
public interface ICommittable
{
    /// <summary>
    /// Commits an offset that is included in a <see cref="CommittableMessage{K,V}"/> 
    /// </summary>
    [Obsolete("Do not use directly. Use Committer.Flow or Committer.Sink instead.")]
    Task Commit();

    /// <summary>
    /// Get a number of processed messages this committable contains
    /// </summary>
    long BatchSize { get; }

    /// <summary>
    /// Add/overwrite an offset position from another committable.
    /// </summary>
    ICommittableOffsetBatch Updated(ICommittable offset);
}

/// <summary>
/// For improved efficiency it is good to aggregate several <see cref="ICommittableOffset"/>,
/// using this class, before <see cref="ICommittable.Commit"/> them.
/// Start with 
/// </summary>
public interface ICommittableOffsetBatch : ICommittable
{
    /// <summary>
    /// Get current offset positions
    /// </summary>
    IImmutableSet<GroupTopicPartitionOffset> Offsets { get; }

    /// <summary>
    /// Returns <c>true</c> if the batch contains no commits.
    /// </summary>
    bool IsEmpty { get; }
}

/// <summary>
/// Included in <see cref="CommittableMessage{K,V}"/>. Makes it possible to
/// commit an offset or aggregate several offsets before committing.
/// Note that the offset position that is committed to Kafka will automatically
/// be one more than the `offset` of the message, because the committed offset
/// should be the next message your application will consume,
/// i.e. lastProcessedMessageOffset + 1.
/// </summary>
public interface ICommittableOffset : ICommittable
{
    /// <summary>
    /// Offset value
    /// </summary>
    GroupTopicPartitionOffset Offset { get; }
}

/// <summary>
/// Extends <see cref="ICommittableOffset"/> with some metadata
/// </summary>
public interface ICommittableOffsetMetadata : ICommittableOffset
{
    /// <summary>
    /// Consumed record metadata
    /// </summary>
    string Metadata { get; }
}