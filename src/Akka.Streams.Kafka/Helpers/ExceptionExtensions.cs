using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    internal static class ExceptionExtensions
    {
        public static string GetCause(this Exception ex)
            => ex switch
            {
                KafkaException {InnerException: null} ke => ke.Error.Reason,
                KafkaException {InnerException: { }} ke => ke.InnerException.Message,
                _ => ex.Message
            };
    }
}