using System.Collections.Generic;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    /// <summary>
    /// Interface for implementing committing consumed messages
    /// </summary>
    internal interface IInternalCommitter 
    {
        /// <summary>
        /// Commit all offsets (of different topics) belonging to the same stage
        /// </summary>
        List<TopicPartitionOffset> Commit();
    }

    /// <summary>
    /// This is a simple committer using kafka consumer directly (not using consumer actor, etc)
    /// </summary>
    internal class KafkaCommitter<K, V> : IInternalCommitter
    {
        private readonly IConsumer<K, V> _consumer;

        public KafkaCommitter(IConsumer<K, V> consumer)
        {
            _consumer = consumer;
        }

        /// <summary>
        /// Commit all offsets (of different topics) belonging to the same stage
        /// </summary>
        public List<TopicPartitionOffset> Commit() => _consumer.Commit();
    }
}