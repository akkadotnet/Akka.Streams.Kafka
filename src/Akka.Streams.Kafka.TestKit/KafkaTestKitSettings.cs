using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Streams.Kafka.TestKit
{
    public class KafkaTestKitSettings
    {
        public static string ConfigPath = "akka.kafka.testkit";

        public static KafkaTestKitSettings Create(ActorSystem system)
            => Create(system.Settings.Config.GetConfig(ConfigPath));

        public static KafkaTestKitSettings Create(Config config)
        {
            return new KafkaTestKitSettings(
                config.GetTimeSpan("cluster-timeout"),
                config.GetTimeSpan("consumer-group-timeout"),
                config.GetTimeSpan("check-interval"));
        }
        
        public KafkaTestKitSettings(TimeSpan clusterTimeout, TimeSpan clusterGroupTimeout, TimeSpan checkInterval)
        {
            ClusterTimeout = clusterTimeout;
            ClusterGroupTimeout = clusterGroupTimeout;
            CheckInterval = checkInterval;
        }

        public TimeSpan ClusterTimeout { get; }
        public TimeSpan ClusterGroupTimeout { get; }
        public TimeSpan CheckInterval { get; }
    }
}