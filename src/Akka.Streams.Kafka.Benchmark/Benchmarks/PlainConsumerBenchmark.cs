using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
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
    public class PlainConsumerBenchmark : KafkaBenchmarkBase
    {
        private IControl _control = null!;
        private ISinkQueue<ConsumeResult<Null, string>> _sink = null!;
        
        public override async Task SetupAsync()
        {
            await base.SetupAsync();
            
            // First produce test data
            var producerSettings = CreateProducerSettings<Null, string>();
            await Source
                .From(Enumerable.Range(1, TestMessageCount))
                .Select(i => new ProducerRecord<Null, string>(TopicName, default, i.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), ActorSystem.Materializer());
            
            // Then set up consumer
            var consumerSettings = CreateConsumerSettings<Null, string>();
            var (control, queue) = KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(TopicName))
                .ToMaterialized(
                    Sink.Queue<ConsumeResult<Null, string>>()
                        .AddAttributes(new Attributes(new Attributes.InputBuffer(2000, 4000))), 
                    Keep.Both)
                .Run(ActorSystem.Materializer());

            _control = control;
            _sink = queue;
        }
        
        [Benchmark]
        public async Task ConsumeMessageAsync()
        {
            var result = await _sink.PullAsync();
            if (!result.HasValue)
                throw new InvalidOperationException("Consumer timed out");
        }
        
        public override async Task CleanupAsync()
        {
            if (_control != null)
                await _control.Shutdown();
                
            await base.CleanupAsync();
        }
    }
} 