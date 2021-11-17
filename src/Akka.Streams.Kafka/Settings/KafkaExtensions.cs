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
            => code switch
            {
                ErrorCode.Local_ValueDeserialization => true,
                ErrorCode.Local_ValueSerialization => true,
                ErrorCode.Local_KeyDeserialization => true,
                ErrorCode.Local_KeySerialization => true,
                _ => false
            };
    }
}
