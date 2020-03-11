using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    /// <summary>
    /// TransactionalSourceHelper
    /// </summary>
    public static class TransactionalSourceHelper
    {
        /// <summary>
        /// Prepares consumer settings to be used in transactional source
        /// </summary>
        public static ConsumerSettings<K, V> PrepareSettings<K, V>(ConsumerSettings<K, V> settings)
        {
            /*
            We set the isolation.level config to read_committed to make sure that any consumed messages are from
            committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
            producers may either use transactional messaging or not at all. So the fetching partitions may have both
            transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
            will still consume non-transactional messages.
            */
            return settings.WithProperty("isolation.level", "read_committed");
        }
    }
}