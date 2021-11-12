using System.Collections.Generic;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Kafka.TestKit.Internal;

namespace Akka.Streams.Kafka.TestKit
{
    public sealed class KafkaTestKitTestContainersSettings
    {
        public const string ConfigPath = "akka.kafka.testkit.test-containers";

        public static KafkaTestKitTestContainersSettings Create(ActorSystem system)
            => Create(system.Settings.Config.GetConfig(ConfigPath));

        public static KafkaTestKitTestContainersSettings Create(Config config)
            => new KafkaTestKitTestContainersSettings(
                config.GetString("confluent-platform-version"),
                config.GetInt("num-brokers"),
                config.GetInt("internal-topics-replication-factor"),
                config.GetBoolean("use-schema-registry"));
        
        public KafkaTestKitTestContainersSettings(
            string confluentPlatformVersion,
            int numBrokers,
            int internalTopicsReplicationFactor,
            bool useSchemaRegistry)
        {
            ConfluentPlatformVersion = confluentPlatformVersion;
            NumBrokers = numBrokers;
            InternalTopicsReplicationFactor = internalTopicsReplicationFactor;
            UseSchemaRegistry = useSchemaRegistry;
        }

        public string ConfluentPlatformVersion { get; }
        public int NumBrokers { get; }
        public int InternalTopicsReplicationFactor { get; }
        public bool UseSchemaRegistry { get; }
        
        //public List<KafkaFixture> ConfigureKafkaConsumer

        public KafkaTestKitTestContainersSettings WithConfluentPlatformVersion(string confluentPlatformVersion)
            => Copy(confluentPlatformVersion: confluentPlatformVersion);
        
        public KafkaTestKitTestContainersSettings WithNumBrokers(int numBrokers)
            => Copy(numBrokers: numBrokers);
        
        public KafkaTestKitTestContainersSettings WithInternalTopicsReplicationFactor(int internalTopicsReplicationFactor)
            => Copy(internalTopicsReplicationFactor: internalTopicsReplicationFactor);
        
        public KafkaTestKitTestContainersSettings WithUseSchemaRegistry(bool useSchemaRegistry)
            => Copy(useSchemaRegistry: useSchemaRegistry);
        
        public KafkaTestKitTestContainersSettings Copy(
            string confluentPlatformVersion = null,
            int? numBrokers = null,
            int? internalTopicsReplicationFactor = null,
            bool? useSchemaRegistry = null)
            => new KafkaTestKitTestContainersSettings(
                confluentPlatformVersion: confluentPlatformVersion ?? ConfluentPlatformVersion,
                numBrokers: numBrokers ?? NumBrokers,
                internalTopicsReplicationFactor: internalTopicsReplicationFactor ?? InternalTopicsReplicationFactor,
                useSchemaRegistry: useSchemaRegistry ?? UseSchemaRegistry);
    }
}