using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Linq;

namespace Akka.Streams.Kafka.Messages
{
    public sealed class CommittableMessage<K, V>
    {
        public CommittableMessage(Message<K, V> record, CommittableOffset commitableOffset)
        {
            Record = record;
            CommitableOffset = commitableOffset;
        }

        public Message<K, V> Record { get; }

        public CommittableOffset CommitableOffset { get; }
    }

    public interface Commitable
    {
        Task<CommittedOffsets> Commit();
    }

    public sealed class CommittableOffset : Commitable
    {
        private readonly Task<CommittedOffsets> _task;

        public CommittableOffset(Task<CommittedOffsets> task, PartitionOffset offset)
        {
            _task = task;
            Offset = offset;
        }

        public PartitionOffset Offset { get; }

        public Task<CommittedOffsets> Commit()
        {
            return _task;
        }
    }

    public sealed class PartitionOffset
    {
        public PartitionOffset(GroupTopicPartition key, long offset)
        {
            Key = key;
            Offset = offset;
        }

        public GroupTopicPartition Key { get; }

        public long Offset { get; }
    }

    public sealed class GroupTopicPartition
    {
        public GroupTopicPartition(string groupId, string topic, int partition)
        {
            GroupId = groupId;
            Topic = topic;
            Partition = partition;
        }

        public string GroupId { get; }

        public string Topic { get; }

        public int Partition { get; }
    }

    public sealed class CommittableOffsetBatch : Commitable
    {
        public CommittableOffsetBatch(Dictionary<GroupTopicPartition, long> offsets, Dictionary<string, Task<CommittedOffsets>> stages)
        {
            Offsets = offsets;
            Stages = stages;
        }

        public Dictionary<GroupTopicPartition, long> Offsets { get; }
        internal Dictionary<string, Task<CommittedOffsets>> Stages { get; }

        public Task<CommittedOffsets> Commit()
        {
            if (Offsets.Count == 0)
                return Task.FromResult(default(CommittedOffsets));
            else
                return Stages.First().Value;
        }

        public CommittableOffsetBatch Updated(CommittableOffset offset)
        {
            var partitionOffset = offset.Offset;
            var key = partitionOffset.Key;

            var newOffsets = new Dictionary<GroupTopicPartition, long>(Offsets);
            newOffsets[key] = partitionOffset.Offset;

            Dictionary<string, Task<CommittedOffsets>> newStages = null;

            if (Stages.TryGetValue(key.GroupId, out var commiter))
            {
                newStages = Stages;
            }
            else
            {
                newStages = new Dictionary<string, Task<CommittedOffsets>>(Stages);
                newStages[key.GroupId] = commiter;
            }

            return new CommittableOffsetBatch(newOffsets, newStages);
        }

        public override string ToString()
        {
            return $"CommittableOffsetBatch({string.Join("->", Offsets)})";
        }

        public static CommittableOffsetBatch Empty { get; } = new CommittableOffsetBatch(new Dictionary<GroupTopicPartition, long>(), new Dictionary<string, Task<CommittedOffsets>>());
    }
}
