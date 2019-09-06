using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions
{
    /// <summary>
    /// Thrown in response to commit/message request commands by consuming actor when in stopping state
    /// </summary>
    public class StoppingException : Exception
    {
        public StoppingException() : base("Kafka consumer is stopping") { }
    }
}