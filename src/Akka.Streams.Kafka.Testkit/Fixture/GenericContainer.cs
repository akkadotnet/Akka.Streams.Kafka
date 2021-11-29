using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Event;
using Docker.DotNet;
using Docker.DotNet.Models;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public abstract class GenericContainer
    {
        private static readonly ContainerStopParameters StopParam = new ContainerStopParameters
        {
            WaitBeforeKillSeconds = 1
        };
        
        private static readonly ContainerRemoveParameters RemoveParam = new ContainerRemoveParameters
        {
            Force = true
        };

        private ILoggingAdapter _log;
        protected DockerClient Client { get; private set; }
        
        public ContainerInfo Info { get; }
        public string ContainerName { get; }
        public string ContainerId { get; private set; }
        public bool IsCreated { get; private set; }
        public bool IsStarted { get; private set; }
        public bool Initialized => Client != null;

        protected GenericContainer(ContainerInfo info)
        {
            Info = info;
            ContainerName = $"{info.ContainerBaseName}-{Guid.NewGuid()}";
        }
        
        protected abstract Task SetupContainerParameters(CreateContainerParameters param);
        protected abstract Task WaitUntilReady(CancellationToken token);

        internal void Initialize(DockerClient client, ILoggingAdapter log)
        {
            if (Initialized)
                throw new Exception("Container already initialized");
            
            Client = client;
            _log = log;
        }
        
        internal void Start()
        {
            StartAsync().Wait();
        }

        internal async Task StartAsync()
        {
            if (!Initialized)
                throw new InvalidOperationException("Container has not been initialized.");
            
            _log.Info($"Starting container [{ContainerId}]");
            
            var success = await Client.Containers.StartContainerAsync(ContainerName, new ContainerStartParameters());
            if (!success)
            {
                throw new Exception($"Failed to start container {ContainerName}");
            }

            IsStarted = true;
            _log.Info($"Waiting for container [{ContainerId}] to be ready");
            using (var cts = new CancellationTokenSource())
            {
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
                var waitTask = WaitUntilReady(cts.Token);
                var completed = Task.WhenAny(timeoutTask, waitTask);
                if (completed == timeoutTask)
                    throw new Exception($"Timed out waiting for container {ContainerId} to start. (10 seconds)");
                cts.Cancel();
            }
        }

        internal async Task CreateAsync()
        {
            if (!Initialized)
                throw new InvalidOperationException("Container has not been initialized.");
            
            _log.Info($"Creating container [{ContainerName}]");
            // Load images, if they not exist yet
            await EnsureImageExistsAsync();
            var parameters = new CreateContainerParameters
            {
                Image = Info.DockerName,
                Name = ContainerName,
                Tty = true,
            };
            await SetupContainerParameters(parameters);
            var response = await Client.Containers.CreateContainerAsync(parameters);
            ContainerId = response.ID;
            foreach (var warning in response.Warnings)
            {
                _log?.Warning(warning);
            }

            IsCreated = true;
        }

        public async Task StopAsync()
        {
            if (!Initialized)
                throw new InvalidOperationException("Container has not been initialized.");
            
            if (!IsCreated) return;
            if (!IsStarted) return;
            _log.Info($"Stopping container [{ContainerId}]");
            await Client.Containers.StopContainerAsync(ContainerId, StopParam);
            IsStarted = false;
        }

        public async Task RemoveAsync()
        {
            if (!Initialized)
                throw new InvalidOperationException("Container has not been initialized.");
            
            if (!IsCreated) return;
            
            if (IsStarted)
                await StopAsync();
            _log.Info($"Removing container [{ContainerId}]");
            await Client.Containers.RemoveContainerAsync(ContainerId, RemoveParam);
            IsCreated = false;
        }
        
        private async Task EnsureImageExistsAsync()
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
                                {Info.DockerName, true}
                            }
                        }
                    }
                });

            if (existingImages.Count == 0)
            {
                await Client.Images.CreateImageAsync(
                    parameters: new ImagesCreateParameters
                    {
                        FromImage = Info.ImageName, Tag = Info.ImageTag
                    }, 
                    authConfig: null,
                    progress: new Progress<JSONMessage>(message =>
                    {
                        if(!string.IsNullOrEmpty(message.ErrorMessage))
                            _log.Error(message.ErrorMessage);
                        else
                            _log.Info($"[{message.ID}][{message.Status}] {message.ProgressMessage}");
                    }));
            }
        }
    }

    public abstract class GenericContainerAsync : GenericContainer
    {
        protected GenericContainerAsync(ContainerInfo info) : base(info)
        {
        }
    }
}