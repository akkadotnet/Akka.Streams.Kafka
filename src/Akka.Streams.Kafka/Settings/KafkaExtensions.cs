using Akka.Configuration;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Settings
{
    public static class KafkaExtensions
    {
        public static Config DefaultSettings =>
            ConfigurationFactory.FromResource<CommitterSettings>("Akka.Streams.Kafka.reference.conf");

        public static bool IsSerializationError(this Error error)
            => error.Code == ErrorCode.Local_ValueDeserialization ||
               error.Code == ErrorCode.Local_ValueSerialization ||
               error.Code == ErrorCode.Local_KeyDeserialization ||
               error.Code == ErrorCode.Local_KeySerialization;

    }
}
