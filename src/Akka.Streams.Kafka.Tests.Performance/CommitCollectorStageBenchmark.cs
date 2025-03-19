using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;
using Akka.Streams.Kafka.Tests.TestKit.Internal;
using Akka.Streams.TestKit;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Tests.Performance
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class CommitCollectorStageBenchmark
    {
        private ActorSystem _system;
        private ActorMaterializer _materializer;
        private CommitterSettings _settings;
        private List<ICommittable> _testData;
        private const int DataSize = 1000;

        [GlobalSetup]
        public void Setup()
        {
            _system = ActorSystem.Create("CommitCollectorStageBenchmark");
            _materializer = ActorMaterializer.Create(_system);
            _testData = new List<ICommittable>();
            
            // Create test data
            for (int i = 0; i < DataSize; i++)
            {
                var offset = ConsumerResultFactory.CommittableOffset(
                    "test-group",
                    "test-topic",
                    partition: i % 4, // Use 4 partitions
                    offset: i,
                    metadata: $"metadata-{i}");
                _testData.Add(offset);
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _materializer.Dispose();
            _system.Terminate().Wait();
        }

        [Params(10, 50, 100)]
        public int MaxBatchSize { get; set; }

        [Params(100, 500, 1000)]
        public int MaxIntervalMs { get; set; }

        [Benchmark]
        public async Task CommitCollectorStageFlow()
        {
            _settings = CommitterSettings.Create(
                maxBatchSize: MaxBatchSize,
                maxInterval: TimeSpan.FromMilliseconds(MaxIntervalMs));

            var source = Source.From(_testData);
            var flow = Flow.FromGraph(new CommitCollectorStage(_settings));
            var sink = Sink.Seq<ICommittableOffsetBatch>();

            var result = await source
                .Via(flow)
                .RunWith(sink, _materializer);
        }
    }
} 