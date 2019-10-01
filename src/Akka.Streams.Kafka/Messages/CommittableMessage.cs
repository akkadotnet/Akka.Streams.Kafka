using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Output element of <see cref="KafkaConsumer.CommittableSource{K,V}"/>.
    /// The offset can be committed via the included <see cref="CommitableOffset"/>.
    /// </summary>
    public sealed class CommittableMessage<K, V>
    {
        public CommittableMessage(ConsumeResult<K, V> record, ICommittableOffset commitableOffset)
        {
            Record = record;
            CommitableOffset = commitableOffset;
        }

        /// <summary>
        /// The consumed record data
        /// </summary>
        public ConsumeResult<K, V> Record { get; }
        /// <summary>
        /// Consumer offset that can be commited
        /// </summary>
        public ICommittableOffset CommitableOffset { get; }
    }
}
