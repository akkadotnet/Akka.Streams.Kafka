using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions
{
    /// <summary>
    /// Calls to <see cref="IInternalCommitter.Commit(System.Collections.Immutable.ImmutableList{Akka.Streams.Kafka.Messages.GroupTopicPartitionOffset})"/> will be failed with this exception if
    /// Kafka doesn't respond within commit timeout
    /// </summary>
    public sealed class CommitTimeoutException : TimeoutException
    {
        public CommitTimeoutException(string message) : base(message) { }
    }
}