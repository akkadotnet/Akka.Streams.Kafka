using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;
using BenchmarkDotNet.Attributes;

namespace Akka.Streams.Kafka.Benchmark
{
    [MemoryDiagnoser]
    public class CommitCollectorStageBenchmark
    {
        private ActorSystem? _system;
        private IMaterializer? _materializer;
        private ICommittable[] _offsets = [];
        private CommitterSettings? _settings;
        
        [Params(1000, 10_000)] // Reduced from 100k to keep benchmark runtime reasonable
        public int TotalMessages { get; set; }
        
        [Params(1, 4, 16)]
        public int PartitionCount { get; set; }
        
        [Params(10, 100)]
        public int BatchSize { get; set; }
        
        [Params(100)]
        public int MaxIntervalMs { get; set; }
        
        [GlobalSetup]
        public void Setup()
        {
            var config = ConfigurationFactory.ParseString("akka.loglevel=WARNING")
                .WithFallback(KafkaExtensions.DefaultSettings);
            _system = ActorSystem.Create("CommitCollectorBenchmark", config);
            _materializer = ActorMaterializer.Create(_system);
            
            _settings = CommitterSettings.Create(_system)
                .WithMaxBatch(BatchSize)
                .WithMaxInterval(TimeSpan.FromMilliseconds(MaxIntervalMs));
            
            // Create test messages distributed across partitions
            _offsets = new ICommittable[TotalMessages];
            var messagesPerPartition = TotalMessages / PartitionCount;
            
            for (var partitionId = 0; partitionId < PartitionCount; partitionId++)
            {
                var groupId = "benchmark-group";
                var topic = "benchmark-topic";
                
                for (var i = 0; i < messagesPerPartition; i++)
                {
                    // Distribute messages across partitions in sequence
                    var position = (i * PartitionCount) + partitionId;
                    if (position < TotalMessages)
                    {
                        var gtp = new GroupTopicPartition(groupId, topic, partitionId);
                        var partitionOffset = new GroupTopicPartitionOffset(gtp, i);
                        
                        _offsets[position] = new CommittableOffset(
                            new KafkaAsyncConsumerCommitter(() => ActorRefs.Nobody, TimeSpan.Zero),
                            partitionOffset,
                            string.Empty);
                    }
                }
            }
        }
        
        [GlobalCleanup]
        public async Task CleanupAsync()
        {
            if (_system != null)
                await _system.Terminate();
        }

        [Benchmark]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark)]
        public async Task ProcessThroughStageAsync()
        {
            if (_system == null || _settings == null || _offsets == null || _materializer == null)
                throw new InvalidOperationException("Benchmark not properly initialized");
            
            // Create and run the stream
            await Source.From(_offsets)
                .Via(Flow.FromGraph(new CommitCollectorStage(_settings)))
                .RunWith(Sink.Ignore<ICommittableOffsetBatch>(), _materializer);
        }
    }
} 