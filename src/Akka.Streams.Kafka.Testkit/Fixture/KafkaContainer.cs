using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Docker.DotNet.Models;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public class KafkaContainer : GenericContainerAsync
    {
        private static readonly ContainerInfo ContainerInfo = new ContainerInfo(
            imageName: "confluentinc/cp-kafka",
            imageTag: "5.3.0",
            containerBaseName: "akka.net-kafka-test");

        private readonly int _id;
        private readonly int _port;
        private readonly int _partitions;
        private readonly int _replicationFactor;
        private readonly string _zookeeperName;
        private readonly int _zookeeperPort;
        
        public KafkaContainer(int id, int port, int partitions, int replicationFactor, string zookeeperName, int zookeeperPort) : base(ContainerInfo)
        {
            _id = id;
            _port = port;
            _partitions = partitions;
            _zookeeperName = zookeeperName;
            _zookeeperPort = zookeeperPort;
            _replicationFactor = replicationFactor;
        }

        protected override Task SetupContainerParameters(CreateContainerParameters param)
        {
            param.ExposedPorts = new Dictionary<string, EmptyStruct>
            {
                {$"{_port}/tcp", new EmptyStruct()},
            };
            param.HostConfig = new HostConfig
            {
                PortBindings = new Dictionary<string, IList<PortBinding>>
                {
                    {
                        $"{_port}/tcp",
                        new List<PortBinding>
                        {
                            new PortBinding
                            {
                                HostPort = _port.ToString()
                            }
                        }
                    },
                    {
                        "29092/tcp",
                        new List<PortBinding>
                        {
                            new PortBinding
                            {
                                HostPort = "29092"
                            }
                        }
                    }
                },
                ExtraHosts = new[] {"localhost:127.0.0.1"},
            };

            var env = new List<string>
            {
                $"KAFKA_ZOOKEEPER_CONNECT={_zookeeperName}:{_zookeeperPort}", // referencing zookeeper container directly in common docker network
                
                $"KAFKA_LISTENERS=BROKER://0.0.0.0:29092,PLAINTEXT://0.0.0.0:{_port}",
                $"KAFKA_ADVERTISED_LISTENERS=BROKER://{ContainerName}:29092,PLAINTEXT://127.0.0.1:{_port}",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME=BROKER",
                
                $"KAFKA_BROKER_ID={_id}",
                $"KAFKA_NUM_PARTITIONS={_partitions}",
                $"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR={_replicationFactor}",
                $"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS={_replicationFactor}",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
                "KAFKA_DELETE_TOPIC_ENABLE=true",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "KAFKA_OPTS=-Djava.net.preferIPv4Stack=True"
            };

            param.Env = env;
            
            return Task.CompletedTask;
        }

        protected override async Task WaitUntilReady(CancellationToken token)
        {
            /*
            var regex = new Regex("\\[KafkaServer id=[0-9]*\\] started \\(kafka.server.KafkaServer\\)");
            using (var stream = await Client.Containers.GetContainerLogsAsync(ContainerId, new ContainerLogsParameters
            {
                ShowStdout = true,
                ShowStderr = true,
            }, token))
            {
                using (var reader = new StreamReader(stream))
                {
                    var ready = false;
                    while (!ready)
                    {
                        var line = await reader.ReadLineAsync();
                        if(!string.IsNullOrEmpty(line))
                            ready = regex.IsMatch(line);
                    }
                }
            }
            */
            var address = IPAddress.Parse("127.0.0.1");
            using (var socket = new TcpClient(AddressFamily.InterNetwork))
            {
                var connected = false;
                while (!connected && !token.IsCancellationRequested)
                {
                    try
                    {
                        await socket.ConnectAsync(address, _port);
                        connected = socket.Connected;
                    }
                    catch (SocketException) { }
                    if (!connected)
                        await Task.Delay(100, token);
                }
            }
        }
    }
}