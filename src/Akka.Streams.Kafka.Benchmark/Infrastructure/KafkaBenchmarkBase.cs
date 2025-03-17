using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
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
        protected ActorSystem? ActorSystem { get; private set; }
        
        private string BenchmarkRunId { get; set; } = null!;
        
        protected string TopicName => $"benchmark-topic-{BenchmarkRunId}";
        
        
        /// <summary>
        /// Has to be randomized on every iteration to ensure we start with a clean slate
        /// </summary>
        protected string GroupId { get; private set; } = null!;
        protected IControl? StreamControl { get; set; }
        
        protected TaskCompletionSource<Done>? DemandControl { get; private set; }
        protected Task<Done>? CompletionTask { get; set; }
        
        public const int TestMessageCount = 100_000;
        protected virtual int PartitionCount => 3;
        
        /// <summary>
        /// Amount of time we're going to give to an individual benchmark iteration to complete
        /// </summary>
        protected virtual TimeSpan CompletionTimeout => TimeSpan.FromSeconds(30);
        
        [GlobalSetup]
        public virtual async Task SetupAsync()
        {
            Console.WriteLine("Setting up Kafka benchmark...");
            
            // Create unique topic and group names for this benchmark run
            BenchmarkRunId = Guid.NewGuid().ToString("N");
            Console.WriteLine($"[Setup] Benchmark run ID: {BenchmarkRunId}");
            Console.WriteLine($"[Setup] Topic name will be: {TopicName}");
            
            // Create topic
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = KafkaServer
            }).Build();
            
            try
            {
                await adminClient.CreateTopicsAsync([
                    new TopicSpecification
                    {
                        Name = TopicName,
                        NumPartitions = PartitionCount,
                        ReplicationFactor = 1
                    }
                ]);
                Console.WriteLine($"[Setup] Successfully created topic: {TopicName}");
            }
            catch (CreateTopicsException e) when (e.Message.Contains("already exists"))
            {
                // Topic already exists - can happen if cleanup failed last time
                // We'll just reuse it
                Console.WriteLine($"[Setup] Topic already exists: {TopicName}");
            }
            
            // Setup actor system
            var config = ConfigurationFactory.ParseString(@"
                akka {
                    log-config-on-start = off
                    stdout-loglevel = INFO
                    loglevel = INFO
                    log-dead-letters = off # no dead letters
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
            
            Console.WriteLine("Kafka benchmark setup complete.");
        }
        
        /**/
        
        [IterationSetup]
        public virtual void IterationSetup()
        {
            DemandControl = new TaskCompletionSource<Done>();
            // need a new group id for each iteration
            GroupId = $"benchmark-group-{BenchmarkRunId}-{Guid.NewGuid():N}";
            Console.WriteLine($"[Iteration] Using topic: {TopicName} with group: {GroupId}");
        }
        
        [IterationCleanup]
        public virtual void IterationCleanup()
        {
            if (StreamControl != null && CompletionTask != null)
            {
                DrainingControl.Create(StreamControl, CompletionTask).DrainAndShutdown().Wait();
                StreamControl = null;
            }
            else if (CompletionTask != null)
            {
                CompletionTask.Wait();
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
            await ActorSystem!.Terminate();
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
                .Create(ActorSystem!, null, null)
                .WithBootstrapServers(KafkaServer)
                .WithGroupId(GroupId)
                
                // Start consuming from the beginning of the topic
                // see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
                
                .WithProperty("auto.offset.reset", "beginning");
        }
        
        protected ProducerSettings<TKey, TValue> CreateProducerSettings<TKey, TValue>()
        {
            return ProducerSettings<TKey, TValue>
                .Create(ActorSystem!, null, null)
                .WithBootstrapServers(KafkaServer);
        }

        /// <summary>
        /// Starts demand flowing through the stream. Call this in your benchmark method.
        /// </summary>
        protected void StartDemand()
        {
            DemandControl?.TrySetResult(Done.Instance);
        }
    }
} 