using Akka.Annotations;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// The API is new and may change in further releases.
    ///
    /// Allows to execute user code when Statistics are emitted.
    /// Use with care: These callbacks are called synchronously on the same thread Kafka's `poll()` is called.
    /// </summary>
    [ApiMayChange]
    public interface IStatisticsHandler
    {
        /// <summary>
        /// Called when statistics are emitted
        /// </summary>
        void OnStatistics<TKey, TValue>(IConsumer<TKey, TValue> consumer, string json);
    }

    /// <summary>
    /// Contains internal imlementations of <see cref="IStatisticsHandler"/>
    /// </summary>
    internal static class StatisticsHandlers
    {
        /// <summary>
        /// Dummy handler which does nothing. Also <see cref="IStatisticsHandler"/>
        /// </summary>
        internal class Empty : IStatisticsHandler
        {
            /// <inheritdoc />
            public void OnStatistics<TKey, TValue>(IConsumer<TKey, TValue> consumer, string json)
            {
            }
        }
    }
}