using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;

namespace Akka.Streams.Kafka.Testkit
{
    public static class ConsumerResultFactory
    {
        public static MockCommitter FakeCommiter { get; } = new MockCommitter();

        public static GroupTopicPartitionOffset PartitionOffset(string groupId, string topic, int partition, long offset)
            => new GroupTopicPartitionOffset(groupId, topic, partition, offset);
        
        public static GroupTopicPartitionOffset PartitionOffset(GroupTopicPartition key, long offset)
            => new GroupTopicPartitionOffset(key, offset);

        internal static CommittableOffset CommittableOffset(string groupId, string topic, int partition, long offset,
            string metadata)
            => CommittableOffset(PartitionOffset(groupId, topic, partition, offset), metadata);

        internal static CommittableOffset CommittableOffset(GroupTopicPartitionOffset partitionOffset, string metadata)
            => new CommittableOffset(FakeCommiter, partitionOffset, metadata);
        
        public class MockCommitter : IInternalCommitter
        {
            public Task Commit(ImmutableList<GroupTopicPartitionOffset> offsets) => Task.CompletedTask;

            public Task Commit(ICommittableOffsetBatch batch) => Task.CompletedTask;
        }
    }
}