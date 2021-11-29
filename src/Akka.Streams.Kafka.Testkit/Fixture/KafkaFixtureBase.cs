using System;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Akka.Event;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public class KafkaFixtureBase: FixtureBase
    {
        private readonly int _brokerCount;
        public int PartitionCount { get; }
        public int ReplicationFactor { get; }
        
        public ImmutableList<KafkaContainer> Brokers { get; protected set; } = ImmutableList<KafkaContainer>.Empty;
        
        public KafkaFixtureBase(int brokerCount, int partitionCount, int replicationFactor, ILoggingAdapter log) : base(log)
        {
            _brokerCount = brokerCount;
            PartitionCount = partitionCount;
            ReplicationFactor = replicationFactor;

            KafkaPorts = new int[brokerCount];
            for (var i = 0; i < brokerCount; ++i)
            {
                KafkaPorts[i] = GetTemporarySocketPort();
            }
            
            ZookeeperPort = GetTemporarySocketPort();
        }
        
        public int[] KafkaPorts { get; }
        public int ZookeeperPort { get; }
        public string BootstrapServer => string.Join(",", KafkaPorts.Take(Math.Min(_brokerCount, 3)).Select(port => $"127.0.0.1:{port}"));

        protected override Task Initialize()
        {
            CreateNetwork($"akka-kafka-network-{Guid.NewGuid()}");
            
            var zookeeper = new ZookeeperContainer(ZookeeperPort);
            RegisterContainer(zookeeper);

            for (var i = 0; i < _brokerCount; ++i)
            {
                var kafka = new KafkaContainer(i+1, KafkaPorts[i], PartitionCount, ReplicationFactor, zookeeper.ContainerName, ZookeeperPort);
                Brokers = Brokers.Add(kafka);
                RegisterContainer(kafka);
            }

            return Task.CompletedTask;
        }
        
        private int GetTemporarySocketPort()
        {
            using (var sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                sock.Bind(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 0));
                return ((IPEndPoint) sock.LocalEndPoint).Port;
            }
        }
    }
}