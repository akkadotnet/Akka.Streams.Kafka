using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [MemoryDiagnoser]
    public class CommittableOffsetBatchBenchmark
    {
        private ICommittableOffset[] _offsets = [];
        private ICommittableOffsetBatch _batch = CommittableOffsetBatch.Empty;
        
        [Params(1, 4, 16)] // Number of partitions
        public int PartitionCount { get; set; }
        
        [Params(1000, 10_000, 100_000)] // Total number of commits
        public int TotalCommits { get; set; }
        
        [GlobalSetup]
        public void Setup()
        {
            // Create test data with offsets distributed across partitions in a co-mingled fashion
            _offsets = new ICommittableOffset[TotalCommits];
            var commitsPerPartition = TotalCommits / PartitionCount;
            
            for (var partitionId = 0; partitionId < PartitionCount; partitionId++)
            {
                var groupId = "benchmark-group";
                var topic = "benchmark-topic";
                var partition = partitionId;
                
                for (var i = 0; i < commitsPerPartition; i++)
                {
                    var offset = i;
                    var gtp = new GroupTopicPartition(groupId, topic, partition);
                    var metadata = string.Empty;
                    var partitionOffset = new GroupTopicPartitionOffset(gtp, offset);
                    
                    // Place this offset in a position that mingles it with other partition offsets
                    var position = (i * PartitionCount) + partitionId;
                    if (position < TotalCommits) // Guard against rounding issues
                    {
                        _offsets[position] = new CommittableOffset(
                            new KafkaAsyncConsumerCommitter(() => ActorRefs.Nobody, TimeSpan.Zero),
                            partitionOffset,
                            metadata);
                    }
                }
            }
            
            // Initialize empty batch
            _batch = CommittableOffsetBatch.Empty;
        }

        [Benchmark]
        [BenchmarkCategory(BenchmarkCategories.MicroBenchmark)]
        public ICommittableOffsetBatch UpdateBatchSequentially()
        {
            var batch = _batch;
            foreach (var offset in _offsets)
            {
                batch = batch.Updated(offset);
            }
            return batch;
        }
    }
} 