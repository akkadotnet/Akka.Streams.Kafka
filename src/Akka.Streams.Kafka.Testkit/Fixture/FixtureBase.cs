using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Event;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public abstract class FixtureBase: IAsyncLifetime
    {
        private readonly DockerClient _client;
        public bool Initialized { get; private set; }
        
        public ImmutableList<GenericContainer> Containers { get; private set; } = ImmutableList<GenericContainer>.Empty;
        public virtual string NetworkName { get; private set; }
        public string NetworkId { get; private set; }
        protected ILoggingAdapter Log { get; }

        protected FixtureBase(ILoggingAdapter log)
        {
            Log = log;

            DockerClientConfiguration config;
                
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix://var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            _client = config.CreateClient();
        }

        public async Task InitializeAsync()
        {
            await Initialize();
            Initialized = true;
            if (Containers.Count == 0)
                throw new InvalidOperationException("No containers are registered in Initialize()");
            
            // Wait until all containers are created, in parallel
            await Task.WhenAll(Containers.Select(c =>
            {
                if(!c.Initialized)
                    c.Initialize(_client, Log);
                return c.CreateAsync();
            }));

            // Create network and wait until all containers are connected, in parallel
            if (!string.IsNullOrEmpty(NetworkName))
            {
                var response = await _client.Networks.CreateNetworkAsync(
                    new NetworksCreateParameters
                    {
                        Name = NetworkName,
                        EnableIPv6 = false
                    });
                NetworkId = response.ID;
                if(!string.IsNullOrEmpty(response.Warning))
                    Log.Warning(response.Warning);

                await Task.WhenAll(Containers.Select(c => _client.Networks.ConnectNetworkAsync(
                    id: NetworkId, 
                    parameters: new NetworkConnectParameters { Container = c.ContainerName })));
            }

            // Start all containers, in parallel if they're async
            var tasks = new List<Task>();
            foreach (var container in Containers)
            {
                if (container is GenericContainerAsync ca)
                {
                    tasks.Add(ca.StartAsync());
                }
                else
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                    container.Start();
                }
            }
            await Task.WhenAll(tasks);
        }

        public async Task DisposeAsync()
        {
            if (_client != null)
            {
                await ResourceCleanupAsync();
                _client.Dispose();
            }
        }

        protected abstract Task Initialize();
        protected void RegisterContainer(GenericContainer container)
        {
            if (Initialized)
                throw new InvalidOperationException("RegisterContainer() can only be called inside Initialize()");
            Containers = Containers.Add(container);
        }

        protected void CreateNetwork(string networkName)
        {
            if (Initialized)
                throw new InvalidOperationException("CreateNetwork() can only be called inside Initialize()");
            NetworkName = networkName;
        }
        
        /// <summary>
        /// This performs cleanup of allocated containers/networks during tests
        /// </summary>
        /// <returns></returns>
        private async Task ResourceCleanupAsync()
        {
            if (_client == null)
                return;

            await Task.WhenAll(Containers
                .Where(c => c.IsCreated)
                .Select(c => c.RemoveAsync()));

            await _client.Networks.DeleteNetworkAsync(NetworkId);
        }
        
    }
}