using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions
{
    /// <summary>
    /// Calls to <see cref="IInternalCommitter.Commit"/> will be failed with this exception if
    /// Kafka doesn't respond within commit timeout
    /// </summary>
    public sealed class CommitTimeoutException : TimeoutException
    {
        public CommitTimeoutException(string message) : base(message) { }
    }
}