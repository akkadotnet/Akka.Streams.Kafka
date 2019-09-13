using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions
{
    /// <summary>
    /// Kafka consumer stages fail with this exception.
    /// </summary>
    public sealed class ConsumerFailed : Exception
    {
        /// <summary>
        /// ConsumerFailed
        /// </summary>
        public ConsumerFailed() : this("Consumer actor failed") { }
        /// <summary>
        /// ConsumerFailed
        /// </summary>
        /// <param name="message">Message</param>
        public ConsumerFailed(string message) : base(message) { }
    }
}