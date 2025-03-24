// -----------------------------------------------------------------------
//  <copyright file="CommitTrigger.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

namespace Akka.Streams.Kafka.Stages.Consumers;

/// <summary>
/// INTERNAL API
///
/// The set of triggers that can be used to commit offsets.
/// </summary>
internal static class CommitTrigger
{
    public interface ITriggeredBy;

    public sealed class BatchSize : ITriggeredBy
    {
        private BatchSize()
        {
        }

        public static BatchSize Instance { get; } = new();
        public override string ToString() => "batch size";
    }

    public sealed class Interval : ITriggeredBy
    {
        private Interval()
        {
        }

        public static Interval Instance { get; } = new();
        public override string ToString() => "interval";
    }

    public sealed class UpstreamClosed : ITriggeredBy
    {
        private UpstreamClosed()
        {
        }

        public static UpstreamClosed Instance { get; } = new();
        public override string ToString() => "upstream closed";
    }

    public sealed class UpstreamFinish : ITriggeredBy
    {
        private UpstreamFinish()
        {
        }

        public static UpstreamFinish Instance { get; } = new();
        public override string ToString() => "upstream finish";
    }

    public sealed class UpstreamFailure : ITriggeredBy
    {
        private UpstreamFailure()
        {
        }

        public static UpstreamFailure Instance { get; } = new();
        public override string ToString() => "upstream failure";
    }
}