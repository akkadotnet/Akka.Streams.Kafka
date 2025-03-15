using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions
{
    /// <summary>
    /// Commita attempts will be failed with this exception if
    /// Kafka doesn't respond within commit timeout
    /// </summary>
    public sealed class CommitTimeoutException : TimeoutException
    {
        public CommitTimeoutException(string message) : base(message) { }
    }
}