using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;

namespace Akka.Streams.Kafka.Cpu.Benchmark
{
    public class DockerSupport
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
        
        public DockerClient Client { get; private set; }
        public int KafkaPort { get; private set; }
        public string KafkaAddress => $"127.0.0.1:{KafkaPort}";
        public int ZookeeperPort { get; private set; }
        
        public async Task SetupContainersAsync()
        {
            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix://var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");
            
            Client = config.CreateClient();

            // Generate random ports for zookeeper and kafka
            ZookeeperPort = TemporaryTcpAddress("127.0.0.1").Port;
            KafkaPort = TemporaryTcpAddress("127.0.0.1").Port;
            
            // Load images, if they not exist yet
            await EnsureImageExistsAsync(ZookeeperImageName, ZookeeperImageTag);
            await EnsureImageExistsAsync(KafkaImageName, KafkaImageTag);

            // Make resources cleanup before allocating new containers/networks
            await ResourceCleanupAsync();

            // create the containers
            await CreateContainerAsync(ZookeeperImageName, ZookeeperImageTag, _zookeeperContainerName, ZookeeperPort, new Dictionary<string, string>()
            {
                ["ZOOKEEPER_CLIENT_PORT"] = ZookeeperPort.ToString(),
                ["ZOOKEEPER_TICK_TIME"] = "2000",
            });
            
            await CreateContainerAsync(KafkaImageName, KafkaImageTag, _kafkaContainerName, KafkaPort, new Dictionary<string, string>()
            {
                ["KAFKA_BROKER_ID"] = "1",
                ["KAFKA_NUM_PARTITIONS"] = "3",
                ["KAFKA_ZOOKEEPER_CONNECT"] = $"{_zookeeperContainerName}:{ZookeeperPort}", // referencing zookeeper container directly in common docker network
                ["KAFKA_LISTENERS"] = $"PLAINTEXT://:{KafkaPort}",
                ["KAFKA_ADVERTISED_LISTENERS"] = $"PLAINTEXT://127.0.0.1:{KafkaPort}",
                ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
                ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
                ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
                ["KAFKA_OPTS"] = "-Djava.net.preferIPv4Stack=True"
            });

            // Setting up network for containers to communicate
            var network = await Client.Networks.CreateNetworkAsync(new NetworksCreateParameters(new NetworkCreate())
            {
                Name = _networkName
            });
            await Client.Networks.ConnectNetworkAsync(network.ID, new NetworkConnectParameters()
            {
                Container = _kafkaContainerName
            });
            await Client.Networks.ConnectNetworkAsync(network.ID, new NetworkConnectParameters()
            {
                Container = _zookeeperContainerName
            });

            // start the containers
            await Client.Containers.StartContainerAsync(_zookeeperContainerName, new ContainerStartParameters());
            await Client.Containers.StartContainerAsync(_kafkaContainerName, new ContainerStartParameters());
        }

        public async Task WaitForKafkaServerAsync()
        {
            // wait until Kafka is ready
            var logStream = await Client.Containers.GetContainerLogsAsync(
                _kafkaContainerName, 
                new ContainerLogsParameters
                {
                    Follow = true,
                    ShowStdout = true,
                    ShowStderr = true
                });

            string line = null;
            const int timeoutInMilis = 60000;
            using (var reader = new StreamReader(logStream))
            {
                var stopwatch = Stopwatch.StartNew();
                while (stopwatch.ElapsedMilliseconds < timeoutInMilis && (line = await reader.ReadLineAsync()) != null)
                {
                    if (line.Contains("started (kafka.server.KafkaServer)"))
                    {
                        break;
                    }
                }
                stopwatch.Stop();
            }
#if NETFX
            logStream.Dispose();
#else
            await logStream.DisposeAsync();
#endif
            
            if (!(line?.Contains("started (kafka.server.KafkaServer)") ?? false))
            {
                await TearDownDockerAsync();
                Client = null;
                throw new Exception("Kafka docker image failed to run.");
            }
            Console.WriteLine("Kafka server started.");
        }

        public async Task TearDownDockerAsync()
        {
            if (Client != null)
            {
                await ResourceCleanupAsync();
                Client.Dispose();
            }
        }
        
        private async Task ResourceCleanupAsync()
        {
            if (Client == null)
                return;
            
            // This task loads list of containers and stops/removes those which were started during tests
            var containerList = await Client.Containers.ListContainersAsync(new ContainersListParameters());
            var containersToStop = containerList
                .Where(
                    c => c.Names.Any(name => name.Contains(KafkaContainerNameBase)) ||
                    c.Names.Any(name => name.Contains(ZookeeperContainerNameBase)));
            foreach (var container in containersToStop)
            {
                await Client.Containers.StopContainerAsync(container.ID, new ContainerStopParameters());
                await Client.Containers.RemoveContainerAsync(container.ID, new ContainerRemoveParameters { Force = true });
            }
            
            // This tasks loads docker networks and removes those which were started during tests
            var networkList = await Client.Networks.ListNetworksAsync(new NetworksListParameters());
            var networks = networkList
                .Where(n => n.Name.Contains(NetworkNameBase));
            foreach (var network in networks)
            {
                await Client.Networks.DeleteNetworkAsync(network.ID);
            }
        }
        
        private async Task CreateContainerAsync(
            string imageName, 
            string imageTag, 
            string containerName,
            int portToExpose,
            Dictionary<string, string> env)
        {
            await Client.Containers.CreateContainerAsync(new CreateContainerParameters
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

        private async Task EnsureImageExistsAsync(string imageName, string imageTag)
        {
            var existingImages = await Client.Images.ListImagesAsync(
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
                await Client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = imageName, Tag = imageTag }, null,
                    new Progress<JSONMessage>(message =>
                    {
                        Console.WriteLine(!string.IsNullOrEmpty(message.ErrorMessage)
                            ? message.ErrorMessage
                            : $"{message.ID} {message.Status} {message.ProgressMessage}");
                    }));
            }
        }
        
        private static IPEndPoint TemporaryTcpAddress(string hostName)
        {
            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                var endpoint = new IPEndPoint(IPAddress.Parse(hostName), 0);
                socket.Bind(endpoint);
                return (IPEndPoint) socket.LocalEndPoint;
            }
        }
        
    }
}