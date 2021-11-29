using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Streams.Kafka.Testkit
{
    public class KafkaTestkitSettings
    {
        public const string ConfigPath = "akka.kafka.testkit";

        public KafkaTestkitSettings(ActorSystem system) : this(system.Settings.Config.GetConfig(ConfigPath))
        { }
        
        public KafkaTestkitSettings(Config config): this(
                config.GetTimeSpan("cluster-timeout"),
                config.GetTimeSpan("consumer-group-timeout"),
                config.GetTimeSpan("check-interval"))
        { }
        
        public KafkaTestkitSettings(TimeSpan clusterTimeout, TimeSpan consumerGroupTimeout, TimeSpan checkInterval)
        {
            ClusterTimeout = clusterTimeout;
            ConsumerGroupTimeout = consumerGroupTimeout;
            CheckInterval = checkInterval;
        }

        public TimeSpan ClusterTimeout { get; }
        public TimeSpan ConsumerGroupTimeout { get; }
        public TimeSpan CheckInterval { get; }
    }
}