using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Util;
using Docker.DotNet;
using Docker.DotNet.Models;
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
        private const string KafkaImageName = "confluentinc/cp-kafka";
        private const string KafkaImageTag = "latest";
        private const string ZookeeperImageName = "confluentinc/cp-zookeeper";
        private const string ZookeeperImageTag = "latest";
        
        private readonly string _kafkaContainerName = $"kafka-{Guid.NewGuid():N}";
        private readonly string _zookeeperContainerName = $"zookeeper-{Guid.NewGuid():N}";
        private readonly string _networkName = $"network-{Guid.NewGuid():N}";
        
        private readonly DockerClient _client;
        
        public KafkaFixture()
        {
            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix://var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            _client = config.CreateClient();
        }
        
        public int KafkaPort { get; private set; }
        public string KafkaServer => $"localhost:{KafkaPort}";

        public async Task InitializeAsync()
        {
            // Load images, if they not exist yet
            await EnsureImageExists(ZookeeperImageName, ZookeeperImageTag);
            await EnsureImageExists(KafkaImageName, KafkaImageTag);

            // Generate random ports for zookeeper and kafka
            var zookeeperPort = ThreadLocalRandom.Current.Next(32000, 33000);
            KafkaPort = ThreadLocalRandom.Current.Next(28000, 29000);

            // create the containers
            await CreateContainer(ZookeeperImageName, ZookeeperImageTag, _zookeeperContainerName, zookeeperPort, new Dictionary<string, string>()
            {
                ["ZOOKEEPER_CLIENT_PORT"] = zookeeperPort.ToString(),
                ["ZOOKEEPER_TICK_TIME"] = "2000",
            });
            await CreateContainer(KafkaImageName, KafkaImageTag, _kafkaContainerName, KafkaPort, new Dictionary<string, string>()
            {
                ["KAFKA_BROKER_ID"] = "1",
                ["KAFKA_ZOOKEEPER_CONNECT"] = $"{_zookeeperContainerName}:{zookeeperPort}", // referencing zookeeper container directly in common docker network
                ["KAFKA_ADVERTISED_LISTENERS"] = $"PLAINTEXT://localhost:{KafkaPort}",
                ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
                ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
                ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1"
            });

            // Setting up network for containers to communicate
            var network = await _client.Networks.CreateNetworkAsync(new NetworksCreateParameters(new NetworkCreate())
            {
                Name = _networkName
            });
            await _client.Networks.ConnectNetworkAsync(network.ID, new NetworkConnectParameters()
            {
                Container = _kafkaContainerName
            });
            await _client.Networks.ConnectNetworkAsync(network.ID, new NetworkConnectParameters()
            {
                Container = _zookeeperContainerName
            });

            // start the containers
            await _client.Containers.StartContainerAsync(_zookeeperContainerName, new ContainerStartParameters());
            await _client.Containers.StartContainerAsync(_kafkaContainerName, new ContainerStartParameters());

            // Provide a 10 second startup delay
            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        public async Task DisposeAsync()
        {
            if (_client != null)
            {
                // Stop Kafka
                await _client.Containers.StopContainerAsync(_kafkaContainerName, new ContainerStopParameters());
                await _client.Containers.RemoveContainerAsync(_kafkaContainerName, new ContainerRemoveParameters { Force = true });
                
                // Stop Zookeeper
                await _client.Containers.StopContainerAsync(_zookeeperContainerName, new ContainerStopParameters());
                await _client.Containers.RemoveContainerAsync(_zookeeperContainerName, new ContainerRemoveParameters { Force = true });

                // Delete network between containers
                await _client.Networks.DeleteNetworkAsync(_networkName);
                
                _client.Dispose();
            }
        }
        
        private async Task CreateContainer(string imageName, string imageTag, string containerName, int portToExpose, Dictionary<string, string> env)
        {
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = $"{imageName}:{imageTag}",
                Name = containerName,
                Tty = true,
                
                ExposedPorts = new Dictionary<string, EmptyStruct>
                {
                    {$"{portToExpose}/tcp", new EmptyStruct()}
                },
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            $"{portToExpose}/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = $"{portToExpose}"
                                }
                            }
                        }
                    },
                    ExtraHosts = new [] { "localhost:127.0.0.1" },
                },
                Env = env.Select(pair => $"{pair.Key}={pair.Value}").ToArray(),
            });
        }

        private async Task EnsureImageExists(string imageName, string imageTag)
        {
            var existingImages = await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = $"{imageName}:{imageTag}" });
            if (existingImages.Count == 0)
            {
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = imageName, Tag = imageTag }, null,
                    new Progress<JSONMessage>(message =>
                    {
                        Console.WriteLine(!string.IsNullOrEmpty(message.ErrorMessage)
                            ? message.ErrorMessage
                            : $"{message.ID} {message.Status} {message.ProgressMessage}");
                    }));
            }
        }
    }
}