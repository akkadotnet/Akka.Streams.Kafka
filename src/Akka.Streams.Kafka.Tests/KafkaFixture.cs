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
        private const string KafkaImageTag = "5.3.0";
        private const string ZookeeperImageName = "confluentinc/cp-zookeeper";
        private const string ZookeeperImageTag = "5.3.0";

        private const string KafkaContainerNameBase = "akka.net-kafka-test";
        private const string ZookeeperContainerNameBase = "akka.net-zookeeper-test";
        private const string NetworkNameBase = "akka.net-network-test";
        private readonly string _kafkaContainerName = $"{KafkaContainerNameBase}-{Guid.NewGuid():N}";
        private readonly string _zookeeperContainerName = $"{ZookeeperContainerNameBase}-{Guid.NewGuid():N}";
        private readonly string _networkName = $"{NetworkNameBase}-{Guid.NewGuid():N}";
        
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

            if (TestsConfiguration.UseExistingDockerContainer)
            {
                KafkaPort = 29092;
            }
        }
        
        public int KafkaPort { get; private set; }
        public string KafkaServer => $"127.0.0.1:{KafkaPort}";

        public async Task InitializeAsync()
        {
            if (TestsConfiguration.UseExistingDockerContainer)
            {
                // When using existing container, no actions should be performed on startup
                return;
            }
            
            // Load images, if they not exist yet
            await EnsureImageExists(ZookeeperImageName, ZookeeperImageTag);
            await EnsureImageExists(KafkaImageName, KafkaImageTag);

            // Generate random ports for zookeeper and kafka
            var zookeeperPort = ThreadLocalRandom.Current.Next(32000, 33000);
            KafkaPort = ThreadLocalRandom.Current.Next(28000, 29000);
            
            // Make resources cleanup before allocating new containers/networks
            await ResourceCleanup();

            // create the containers
            await CreateContainer(ZookeeperImageName, ZookeeperImageTag, _zookeeperContainerName, zookeeperPort, new Dictionary<string, string>()
            {
                ["ZOOKEEPER_CLIENT_PORT"] = zookeeperPort.ToString(),
                ["ZOOKEEPER_TICK_TIME"] = "2000",
            });
            await CreateContainer(KafkaImageName, KafkaImageTag, _kafkaContainerName, KafkaPort, new Dictionary<string, string>()
            {
                ["KAFKA_BROKER_ID"] = "1",
                ["KAFKA_NUM_PARTITIONS"] = "3",
                ["KAFKA_ZOOKEEPER_CONNECT"] = $"{_zookeeperContainerName}:{zookeeperPort}", // referencing zookeeper container directly in common docker network
                ["KAFKA_LISTENERS"] = $"PLAINTEXT://:{KafkaPort}",
                ["KAFKA_ADVERTISED_LISTENERS"] = $"PLAINTEXT://127.0.0.1:{KafkaPort}",
                ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
                ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
                ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
                ["KAFKA_OPTS"] = "-Djava.net.preferIPv4Stack=True"
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
                // Shutdown running containers only when we were not using pre-existing container
                if (!TestsConfiguration.UseExistingDockerContainer)
                {
                    await ResourceCleanup();
                }
                
                _client.Dispose();
            }
        }

        /// <summary>
        /// This performs cleanup of allocated containers/networks during tests
        /// </summary>
        /// <returns></returns>
        private async Task ResourceCleanup()
        {
            if (_client == null)
                return;
            
            // This task loads list of containers and stops/removes those which were started during tests
            var containerCleanupTask = _client.Containers.ListContainersAsync(new ContainersListParameters()).ContinueWith(
                async t =>
                {
                    if (t.IsFaulted)
                        return;

                    var containersToStop = t.Result.Where(c => c.Names.Any(name => name.Contains(KafkaContainerNameBase)) ||
                                                               c.Names.Any(name => name.Contains(ZookeeperContainerNameBase)));

                    var stopTasks = containersToStop.Select(async container =>
                    {
                        await _client.Containers.StopContainerAsync(container.ID, new ContainerStopParameters());
                        await _client.Containers.RemoveContainerAsync(container.ID, new ContainerRemoveParameters { Force = true });
                    });
                    await Task.WhenAll(stopTasks);
                }).Unwrap();
            
            // This tasks loads docker networks and removes those which were started during tests
            var networkCleanupTask = _client.Networks.ListNetworksAsync(new NetworksListParameters()).ContinueWith(
                async t =>
                {
                    if (t.IsFaulted)
                        return;

                    var networksToDelete = t.Result.Where(network => network.Name.Contains(NetworkNameBase));

                    var deleteTasks = networksToDelete.Select(network => _client.Networks.DeleteNetworkAsync(network.ID));

                    await Task.WhenAll(deleteTasks);
                });

            try
            {
                // Wait until cleanup is finished
                await Task.WhenAll(containerCleanupTask, networkCleanupTask);
            }
            catch { /* If the cleanup failes, this is not the reason to fail tests */ }
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
            var existingImages = await _client.Images.ListImagesAsync(
                new ImagesListParameters
                {
                    Filters = new Dictionary<string, IDictionary<string, bool>>
                    {
                        {
                            "reference",
                            new Dictionary<string, bool>
                            {
                                {$"{imageName}:{imageTag}", true}
                            }
                        }
                    }
                });

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