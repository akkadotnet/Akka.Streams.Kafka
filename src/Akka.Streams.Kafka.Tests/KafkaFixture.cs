using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Util;
using Docker.DotNet;
using Docker.DotNet.Models;
using Testcontainers.Kafka;
using Xunit;

namespace Akka.Streams.Kafka.Tests
{
    [CollectionDefinition(Name)]
    public sealed class KafkaSpecsFixture : ICollectionFixture<KafkaFixture>
    {
        public const string Name = "KafkaSpecs";
    }
    
    public class KafkaFixture : IAsyncLifetime
    {
        private readonly KafkaContainer _container;
        
        public KafkaFixture()
        {
            _container = new KafkaBuilder()
                .WithImage("confluentinc/cp-kafka:6.1.9")
                .WithEnvironment(new Dictionary<string, string>
                {
                    ["KAFKA_BROKER_ID"] = "1",
                    ["KAFKA_NUM_PARTITIONS"] = "3",
                    ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
                    ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
                    ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
                })
                .Build();
            
            if (TestsConfiguration.UseExistingDockerContainer)
            {
                KafkaPort = 29092;
            }
        }
        
        public int KafkaPort { get; private set; }
        public string KafkaServer => $"{_container.Hostname}:{KafkaPort}";

        public const int KafkaReplicationFactor = 1;
        public const int KafkaPartitions = 3;

        public async Task InitializeAsync()
        {
            if (TestsConfiguration.UseExistingDockerContainer)
            {
                // When using existing container, no actions should be performed on startup
                return;
            }
            
            await _container.StartAsync();
            KafkaPort = _container.GetMappedPublicPort(KafkaBuilder.KafkaPort);
        }

        public async Task DisposeAsync()
        {
            await _container.DisposeAsync();
        }
    }
}