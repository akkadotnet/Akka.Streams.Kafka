using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Testkit
{
    // TODO: Need testing, check if little endian/big endianness is correct
    /// <summary>
    /// Specialized deserializer for MemberAssignment.
    /// C# AdminClient returns raw bytes that needs to be deserialized into a proper object
    /// </summary>
    public class MemberAssignment
    {
        public ImmutableHashSet<TopicPartition> TopicPartitions { get; }

        public MemberAssignment(ImmutableHashSet<TopicPartition> topicPartitions)
        {
            TopicPartitions = topicPartitions;
        }

        private static ImmutableHashSet<TopicPartition> DeserializeTopicPartitions(BinaryReader reader)
        {
            var topic = reader.ReadString();
            var partitionLength = reader.ReadInt32BE();
            var partitions = new HashSet<TopicPartition>();
            for (var i = 0; i < partitionLength; ++i)
            {
                partitions.Add(new TopicPartition(topic, reader.ReadInt32BE()));
            }

            return partitions.ToImmutableHashSet();
        }

        public static MemberAssignment Deserialize(byte[] raw)
        {
            if (raw == null || raw.Length == 0)
                return null;

            var tps = new HashSet<TopicPartition>();
            using var stream = new MemoryStream(raw);
            using var reader = new BinaryReader(stream);
            
            var version = reader.ReadInt16BE();
            var assignCount = reader.ReadInt32BE();
            for (var i = 0; i < assignCount; ++i)
            {
                tps.UnionWith(DeserializeTopicPartitions(reader));
            }

            return new MemberAssignment(tps.ToImmutableHashSet());
        }
    }
}