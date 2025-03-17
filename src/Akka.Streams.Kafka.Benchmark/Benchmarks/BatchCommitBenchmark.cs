using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Benchmark.Infrastructure;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark.Benchmarks
{
    [MinWarmupCount(3)]
    [MaxWarmupCount(5)]
    [MinIterationCount(3)]
    [MaxIterationCount(5)]
    public class BatchCommitBenchmark : KafkaBenchmarkBase
    {
        private IControl _control = null!;
        private ISinkQueue<ICommittableOffsetBatch> _sink = null!;
        
        [Params(10, 100, 1000)]
        public int BatchSize { get; set; }
        
        public override async Task SetupAsync()
        {
            await base.SetupAsync();
            
            // First produce test data
            var producerSettings = CreateProducerSettings<Null?, string>();
            await Source
                .From(Enumerable.Range(1, TestMessageCount))
                .Select(i => new ProducerRecord<Null?, string>(TopicName, null, i.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), ActorSystem.Materializer());
            
            // Then set up consumer with batch commit
            var consumerSettings = CreateConsumerSettings<Null, string>();
            var committerSettings = CommitterSettings.Create(ActorSystem)
                .WithMaxBatch(BatchSize);
            
            var (control, queue) = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(TopicName))
                .Select(message => message.CommitableOffset as ICommittable)
                .Via(Committer.BatchFlow(committerSettings))
                .ToMaterialized(Sink.Queue<ICommittableOffsetBatch>(), Keep.Both)
                .Run(ActorSystem.Materializer());

            _control = control;
            _sink = queue;
        }
        
        [Benchmark]
        public async Task ConsumeAndCommitBatchAsync()
        {
            var messagesProcessed = 0;
            while (messagesProcessed < TestMessageCount)
            {
                var result = await _sink.PullAsync();
                if (result.HasValue)
                {
                    await result.Value.Commit();
                    messagesProcessed += (int)result.Value.BatchSize;
                }
            }
        }
        
        public override async Task CleanupAsync()
        {
            await _control.Shutdown();
            await base.CleanupAsync();
        }
    }
} 