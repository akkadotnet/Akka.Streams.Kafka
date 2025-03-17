using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Benchmark
{
    public abstract class BenchmarkBase
    {
        protected const string KafkaBootstrapServers = "localhost:29092";
        public ActorSystem ActorSystem { get; private set; } = null!;
        
        protected BenchmarkBase()
        {
        }
        
        public Task SetupKafkaAsync()
        {
            _uuid = Guid.NewGuid().ToString();
            KafkaTopic = $"topic-1-{_uuid}";
            KafkaGroup = $"group-1-{_uuid}";
            return Task.CompletedTask;
        }

        public async Task SetupAkkaAsync()
        {
            await SetupActorSystemsAsync();
        }

        public async Task TearDownAkkaAsync()
        {
            await TeardownActorSystemsAsync();
        }
        
        public Task TearDownKafkaAsync()
        {
            return Task.CompletedTask;
        }

        #region Akka methods

        private string _uuid = null!;
        public ActorSystem ConsumerSystem { get; private set; } = null!;
        public string KafkaTopic { get; private set; } = null!;
        public string KafkaGroup { get; private set; } = null!;

        private Task SetupActorSystemsAsync()
        {
            Console.WriteLine("Starting Akka ActorSystems");
            
            var config = ConfigurationFactory.ParseString("""
                                                          
                                                                    akka {
                                                                      log-config-on-start = off
                                                                      stdout-loglevel = INFO
                                                                      loglevel = ERROR
                                                                      actor {
                                                                        debug {
                                                                            receive = on
                                                                            autoreceive = on
                                                                            lifecycle = on
                                                                            event-stream = on
                                                                            unhandled = on
                                                                        }
                                                                      }          
                                                                    }
                                                          """)
                .WithFallback(KafkaExtensions.DefaultSettings);
            
            ConsumerSystem = ActorSystem.Create("akka-kafka-consumer", config);
            Console.WriteLine("ActorSystems started");
            return Task.CompletedTask;
        }

        private async Task TeardownActorSystemsAsync()
        {
            try
            {
                await ConsumerSystem.Terminate();
            }
            catch
            {
                // ignored
            }
        }

        #endregion
    }
}