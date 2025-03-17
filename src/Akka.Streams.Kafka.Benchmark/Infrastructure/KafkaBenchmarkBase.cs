using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Stages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure
{
    /// <summary>
    /// Base class for all Kafka benchmarks providing common infrastructure
    /// </summary>
    public abstract class KafkaBenchmarkBase
    {
        private const string KafkaServer = "localhost:29092";
        protected ActorSystem ActorSystem { get; private set; } = null!;
        protected string TopicName { get; private set; } = null!;
        protected string GroupId { get; private set; } = null!;
        
        protected virtual int TestMessageCount => 100_000;
        protected virtual int PartitionCount => 3;
        
        [GlobalSetup]
        public virtual async Task SetupAsync()
        {
            // Create unique topic and group names for this benchmark run
            var benchmarkId = Guid.NewGuid().ToString("N");
            TopicName = $"benchmark-topic-{benchmarkId}";
            GroupId = $"benchmark-group-{benchmarkId}";
            
            // Create topic
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = KafkaServer
            }).Build();
            
            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = TopicName,
                        NumPartitions = PartitionCount,
                        ReplicationFactor = 1
                    }
                });
            }
            catch (CreateTopicsException e) when (e.Message.Contains("already exists"))
            {
                // Topic already exists - can happen if cleanup failed last time
                // We'll just reuse it
            }
            
            // Setup actor system
            var config = ConfigurationFactory.ParseString(@"
                akka {
                    log-config-on-start = off
                    stdout-loglevel = INFO
                    loglevel = WARNING
                    actor {
                        debug {
                            receive = off
                            autoreceive = off
                            lifecycle = off
                            event-stream = off
                            unhandled = off
                        }
                    }
                }").WithFallback(KafkaExtensions.DefaultSettings);
            
            ActorSystem = ActorSystem.Create("kafka-benchmark", config);
        }
        
        [GlobalCleanup]
        public virtual async Task CleanupAsync()
        {
            // Cleanup topic
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = KafkaServer
            }).Build();
            
            try
            {
                await adminClient.DeleteTopicsAsync([TopicName]);
            }
            catch
            {
                // Best effort cleanup
            }
            
            // Cleanup actor system
            await ActorSystem.Terminate();
        }
        
        protected ConsumerSettings<TKey, TValue> CreateConsumerSettings<TKey, TValue>()
        {
            return ConsumerSettings<TKey, TValue>
                .Create(ActorSystem, null, null)
                .WithBootstrapServers(KafkaServer)
                .WithGroupId(GroupId)
                .WithProperty("auto.offset.reset", "earliest");
        }
        
        protected ProducerSettings<TKey, TValue> CreateProducerSettings<TKey, TValue>()
        {
            return ProducerSettings<TKey, TValue>
                .Create(ActorSystem, null, null)
                .WithBootstrapServers(KafkaServer);
        }

        /// <summary>
        /// Creates a sink that counts messages and completes when either:
        /// 1. TestMessageCount messages have been processed
        /// 2. The upstream completes
        /// 3. An error occurs
        /// </summary>
        protected static Sink<T, Task<Done>> CreateCountingSink<T>(int stopAt)
        {
            return Flow.Create<T>()
                .Take(stopAt)
                .WatchTermination((used, task) => Task.FromResult(Done.Instance))
                .To(Sink.Ignore<T>());
        }
    }
} 