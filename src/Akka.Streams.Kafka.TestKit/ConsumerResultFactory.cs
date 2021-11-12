using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.TestKit
{
    public class ConsumerResultFactory
    {
        public MockCommitter FakeCommiter => new MockCommitter();

        public GroupTopicPartitionOffset PartitionOffset(string groupId, string topic, int partition, long offset)
            => new GroupTopicPartitionOffset(groupId, topic, partition, offset);
        
        public GroupTopicPartitionOffset PartitionOffset(GroupTopicPartition key, long offset)
            => new GroupTopicPartitionOffset(key, offset);

        internal CommittableOffset CommittableOffset(string groupId, string topic, int partition, long offset,
            string metadata)
            => CommittableOffset(PartitionOffset(groupId, topic, partition, offset), metadata);
        
        internal CommittableOffset CommittableOffset(GroupTopicPartitionOffset partitionOffset, string metadata)
            => new CommittableOffset(FakeCommiter, )
        
        public class MockCommitter : IInternalCommitter
        {
            public Task Commit(ImmutableList<GroupTopicPartitionOffset> offsets) => Task.CompletedTask;

            public Task Commit(ICommittableOffsetBatch batch) => Task.CompletedTask;
        }
    }
}