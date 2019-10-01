using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Stages.Consumers;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Implementation of the offset, contained in <see cref="CommittableMessage{K,V}"/>.
    /// Can be commited via <see cref="Commit"/> method.
    /// </summary>
    internal sealed class CommittableOffset : ICommittableOffsetMetadata
    {
        /// <inheritdoc />
        public long BatchSize => 1;
        /// <summary>
        /// Offset value
        /// </summary>
        public GroupTopicPartitionOffset Offset { get; }
        /// <summary>
        /// Cosumed record metadata
        /// </summary>
        public string Metadata { get; }
        /// <summary>
        /// Committer
        /// </summary>
        public IInternalCommitter Committer { get; }

        public CommittableOffset(IInternalCommitter committer, GroupTopicPartitionOffset offset, string metadata)
        {
            Committer = committer;
            Offset = offset;
            Metadata = metadata;
        }

        /// <summary>
        /// Commits offset to Kafka
        /// </summary>
        public Task Commit()
        {
            return Committer.Commit(ImmutableList.Create(Offset));
        }
    }
}