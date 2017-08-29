using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    public sealed class CommittableMessage<K, V>
    {
        public CommittableMessage(Message<K, V> record, CommitableOffset commitableOffset)
        {
            Record = record;
            CommitableOffset = commitableOffset;
        }

        public Message<K, V> Record { get; }

        public CommitableOffset CommitableOffset { get; }
    }

    public class CommitableOffset
    {
        private readonly Action _action;

        public CommitableOffset(Action action, PartitionOffset offset)
        {
            _action = action;
            Offset = offset;
        }

        public PartitionOffset Offset { get; }

        public Task Commit()
        {
            return Task.CompletedTask;
        }
    }

    public class PartitionOffset
    {
        public PartitionOffset(string groupId, string topic, int partition, Offset offset)
        {
            GroupId = groupId;
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        public string GroupId { get; }

        public string Topic { get; }

        public int Partition { get; }

        public Offset Offset { get; }
    }
}
