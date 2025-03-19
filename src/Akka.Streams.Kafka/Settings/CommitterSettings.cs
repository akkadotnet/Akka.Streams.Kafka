// -----------------------------------------------------------------------
//  <copyright file="CommitterSettings.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Kafka.Messages;

namespace Akka.Streams.Kafka.Settings;

/// <summary>
/// Determines when to commit offsets
/// </summary>
public interface ICommitWhen;

public static class CommitWhen
{
    /// <summary>
    /// Commit as soon as a <see cref="ICommittableOffset"/> is observed.
    /// </summary>
    public sealed class OffsetFirstObserved : ICommitWhen
    {
        public static OffsetFirstObserved Instance { get; } = new();

        private OffsetFirstObserved()
        {
        }
    }

    /// <summary>
    /// Commit the previous offset as soon as a new <see cref="ICommittableOffset"/> is observed.
    /// </summary>
    public sealed class NextOffsetObserved : ICommitWhen
    {
        public static NextOffsetObserved Instance { get; } = new();

        private NextOffsetObserved()
        {
        }
    }

    public static ICommitWhen ValueOf(string s) =>
        s.ToLowerInvariant() switch
        {
            "offset-first-observed" => OffsetFirstObserved.Instance,
            "next-offset-observed" => NextOffsetObserved.Instance,
            _ => throw new ArgumentException(
                $"Allow values are offset-first-observed, next-offset-observed.Received: {s}")
        };
}

/// <summary>
/// Settings for committer. See 'akka.kafka.committer' section in reference.conf.
/// </summary>
public sealed record CommitterSettings
{
    /// <summary>
    /// CommitterSettings
    /// </summary>
    /// <param name="maxBatch">Max commit batch size</param>
    /// <param name="maxInterval">Max commit interval</param>
    /// <param name="parallelism">Level of parallelism</param>
    /// <param name="when">The strategy for when to commit offsets</param>
    public CommitterSettings(int maxBatch, TimeSpan maxInterval, int parallelism, ICommitWhen when)
    {
        MaxBatch = maxBatch;
        MaxInterval = maxInterval;
        Parallelism = parallelism;
        When = when;
    }

    /// <summary>
    /// Creates committer settings
    /// </summary>
    /// <param name="system">Actor system for stage materialization</param>
    /// <returns>Committer settings</returns>
    public static CommitterSettings Create(ActorSystem system)
    {
        var config = system.Settings.Config.GetConfig("akka.kafka.committer");
        return Create(config);
    }

    /// <summary>
    /// Creates committer settings
    /// </summary>
    /// <param name="config">Config to load properties from</param>
    /// <returns>Committer settings</returns>
    public static CommitterSettings Create(Config config)
    {
        var maxBatch = config.GetInt("max-batch");
        var maxInterval = config.GetTimeSpan("max-interval");
        var parallelism = config.GetInt("parallelism");
        var commitWhen = CommitWhen.ValueOf(config.GetString("when"));
        return new CommitterSettings(maxBatch, maxInterval, parallelism, commitWhen);
    }

    /// <summary>
    /// Max commit batch size
    /// </summary>
    public int MaxBatch { get; init; }

    /// <summary>
    /// Max commit interval
    /// </summary>
    public TimeSpan MaxInterval { get; init; }

    /// <summary>
    /// Level of parallelism
    /// </summary>
    public int Parallelism { get; init; }

    public ICommitWhen When { get; init; }

    /// <summary>
    /// Sets max batch size
    /// </summary>
    public CommitterSettings WithMaxBatch(int maxBatch) => this with { MaxBatch = maxBatch };

    /// <summary>
    /// Sets max commit interval
    /// </summary>
    public CommitterSettings WithMaxInterval(TimeSpan maxInterval) => this with { MaxInterval = maxInterval };

    /// <summary>
    /// Sets parallelism level
    /// </summary>
    public CommitterSettings WithParallelism(int parallelism) => this with { Parallelism = parallelism };

    /// <summary>
    /// Sets the strategy for when we commit.
    /// </summary>
    public CommitterSettings WithCommitWhen(ICommitWhen when) => this with { When = when };
}