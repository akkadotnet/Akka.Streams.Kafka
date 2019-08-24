using System.Collections.Generic;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    public interface IInternalCommitter 
    {
        /// <summary>
        /// Commit all offsets (of different topics) belonging to the same stage
        /// </summary>
        List<TopicPartitionOffset> Commit();
    }

    /// <summary>
    /// This is a simple committer using kafka consumer directly (not consumer actor, etc)
    /// </summary>
    public class KafkaCommitter<K, V> : IInternalCommitter
    {
        private readonly IConsumer<K, V> _consumer;

        public KafkaCommitter(IConsumer<K, V> consumer)
        {
            _consumer = consumer;
        }

        public List<TopicPartitionOffset> Commit() => _consumer.Commit();
    }
}