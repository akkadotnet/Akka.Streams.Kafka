using System;
using System.Linq;
using Akka.Actor;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;

namespace Akka.Streams.Kafka.Benchmark
{
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    [MemoryDiagnoser]
    public class CommittableOffsetBatchBenchmark
    {
        private ICommittableOffset _originalOffset = null!;
        private ICommittableOffsetBatch _originalBatch = CommittableOffsetBatch.Empty;
        
        private ICommittableOffset _ingestedOffset = null!;
        private ICommittableOffsetBatch _ingestedBatch = CommittableOffsetBatch.Empty;
        
        [Params(1, 4, 16)] // Number of partitions
        public int PartitionCount { get; set; }
        
        [Params(1000, 10_000, 100_000)] // Total number of commits
        public int TotalCommits { get; set; }
        
        [GlobalSetup]
        public void Setup()
        {
            const string groupId = "benchmark-group";
            const string topic = "benchmark-topic";
            
            // Create test data with offsets distributed across partitions in a co-mingled fashion
            var newOffsets = new ICommittableOffset[TotalCommits];
            var commitsPerPartition = TotalCommits / PartitionCount;
            var committer = new KafkaAsyncConsumerCommitter(() => ActorRefs.Nobody, TimeSpan.Zero);
            for (var i = 0; i < commitsPerPartition; i++)
            {
                for (var partitionId = 0; partitionId < PartitionCount; partitionId++)
                {
                    var gtp = new GroupTopicPartition(groupId, topic, partitionId);
                    var metadata = string.Empty;
                    var partitionOffset = new GroupTopicPartitionOffset(gtp, i);
                    
                    // Place this offset in a position that mingles it with other partition offsets
                    var position = (i * PartitionCount) + partitionId;
                    if (position < TotalCommits) // Guard against rounding issues
                    {
                        newOffsets[position] = new CommittableOffset(
                            committer,
                            partitionOffset,
                            metadata);
                    }
                }
            }
            
            // Original batch
            var originalBatchOffsets = newOffsets.Take(PartitionCount);
            _originalBatch = CommittableOffsetBatch.Create(originalBatchOffsets);
            
            // Ingested batch
            var ingestedBatchOffsets = newOffsets.Skip(PartitionCount);
            _ingestedBatch = CommittableOffsetBatch.Create(ingestedBatchOffsets);
            
            // Original offset - just use a single partition
            var offsetsInFirstPartition = newOffsets.Where(o => o.Offset.GroupTopicPartition.Partition == 0)
                .Take(2).ToList();
            _originalOffset = offsetsInFirstPartition.First();
            
            // New offset
            _ingestedOffset = offsetsInFirstPartition.Last();
        }

        [Benchmark(Baseline = true)]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark, "SingleCommit")]
        public ICommittableOffsetBatch UpdateSingleCommitWithSingleCommit() => _originalOffset.Updated(_ingestedOffset);
        
        [Benchmark]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark, "SingleCommit")]
        public ICommittableOffsetBatch UpdateSingleCommitWithBatch() => _originalOffset.Updated(_ingestedBatch);
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark, "Batch")]
        public ICommittableOffsetBatch UpdateBatchWithSingleCommit() => _originalBatch.Updated(_ingestedOffset);
        
        [Benchmark]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark, "Batch")]
        public ICommittableOffsetBatch UpdateBatchWithBatch() => _originalBatch.Updated(_ingestedBatch);
    }
} 