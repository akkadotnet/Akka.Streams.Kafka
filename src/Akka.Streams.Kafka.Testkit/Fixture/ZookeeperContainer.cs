using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Docker.DotNet.Models;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public class ZookeeperContainer: GenericContainerAsync
    {
        private static readonly ContainerInfo ContainerInfo = new ContainerInfo(
            imageName: "confluentinc/cp-zookeeper",
            imageTag: "5.3.0",
            containerBaseName: "akka.net-zookeeper-test");
        
        private int _port;

        public ZookeeperContainer(int port) : base(ContainerInfo)
        {
            _port = port;
        }

        protected override Task SetupContainerParameters(CreateContainerParameters param)
        {
            param.ExposedPorts = new Dictionary<string, EmptyStruct>
            {
                {$"{_port}/tcp", new EmptyStruct()}
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
                    }
                },
                ExtraHosts = new[] {"localhost:127.0.0.1"},
            };
            param.Env = new[]
            {
                $"ZOOKEEPER_CLIENT_PORT={_port}",
                "ZOOKEEPER_TICK_TIME=2000",
            };
            
            return Task.CompletedTask;
        }

        protected override async Task WaitUntilReady(CancellationToken token)
        {
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

            await Task.Delay(1000, token);
        }
    }
}