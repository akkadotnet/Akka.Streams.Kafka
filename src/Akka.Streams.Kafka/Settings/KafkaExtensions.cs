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
            => error.Code.IsSerializationError();
        
        public static bool IsSerializationError(this ErrorCode code)
            => code == ErrorCode.Local_ValueDeserialization ||
               code == ErrorCode.Local_ValueSerialization ||
               code == ErrorCode.Local_KeyDeserialization ||
               code == ErrorCode.Local_KeySerialization;
    }
}
