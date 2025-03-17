using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
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
        protected IControl? StreamControl { get; private set; }
        
        protected TaskCompletionSource<Done>? DemandControl { get; private set; }
        protected Task<Done>? CompletionTask { get; private set; }
        
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
            
            // Populate test data if needed (for consumer benchmarks)
            await PopulateTestDataAsync();
        }

        /// <summary>
        /// Override this method to populate test data in global setup
        /// No-op by default (for producer benchmarks)
        /// </summary>
        protected virtual Task PopulateTestDataAsync() => Task.CompletedTask;
        
        [IterationSetup]
        public virtual async Task IterationSetupAsync()
        {
            DemandControl = new TaskCompletionSource<Done>();
            
            // Setup stream but don't start demand
            await SetupStreamAsync();
        }
        
        /// <summary>
        /// Override this to setup your stream configuration.
        /// The stream should be fully materialized but not yet demanding data.
        /// Use CreateDemandControlFlow() to control when data starts flowing.
        /// </summary>
        protected abstract Task SetupStreamAsync();
        
        [IterationCleanup]
        public virtual async Task IterationCleanupAsync()
        {
            if (StreamControl != null)
            {
                await StreamControl.Shutdown();
                StreamControl = null;
            }
            
            if (CompletionTask != null)
            {
                await CompletionTask;
                CompletionTask = null;
            }
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
        
        protected async Task<int> GenerateTestDataStringsAsync()
        {
            var producerSettings = CreateProducerSettings<Null?, string>();
            await Source
                .From(Enumerable.Range(1, TestMessageCount))
                .Select(i => new ProducerRecord<Null?, string>(TopicName, null, i.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), ActorSystem.Materializer());

            return TestMessageCount;
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
        protected Sink<T, Task<Done>> CreateCountingSink<T>()
        {
            return CreateCountingSink<T>(TestMessageCount);
        }

        /// <summary>
        /// Creates a sink that counts messages and completes when either:
        /// 1. The specified number of messages have been processed
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

        /// <summary>
        /// Creates a flow that controls demand. Insert this flow in your stream to control when data starts flowing.
        /// Call StartDemand() to begin processing.
        /// </summary>
        protected Flow<T, T, NotUsed> CreateDemandControlFlow<T>(Task startSignal)
        {
            return Flow.Create<T>()
                .SelectAsync(1, async elem =>
                {
                    await startSignal;
                    return elem;
                });
        }

        /// <summary>
        /// Starts demand flowing through the stream. Call this in your benchmark method.
        /// </summary>
        protected void StartDemand()
        {
            DemandControl?.Success(Done.Instance);
        }
    }
} 