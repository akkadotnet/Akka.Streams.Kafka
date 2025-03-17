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
        private Task<Done> _completion = null!;
        
        protected override Task PopulateTestDataAsync() => 
            GenerateTestDataStringsAsync();
        
        public override async Task SetupAsync()
        {
            await base.SetupAsync();
            
            // First produce test data
            await GenerateTestDataStringsAsync();
            
            // Then set up consumer
            var consumerSettings = CreateConsumerSettings<Null, string>();
            var (control, completion) = KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(TopicName))
                .ToMaterialized(CreateCountingSink<ConsumeResult<Null, string>>(TestMessageCount), Keep.Both)
                .Run(ActorSystem.Materializer());

            _control = control;
            _completion = completion;
        }
        
        public Task ConsumeMessageAsync() => _completion;

        protected override Task SetupStreamAsync() => throw new NotImplementedException();

        public override async Task CleanupAsync()
        {
            await _control.Shutdown();
            await base.CleanupAsync();
        }
    }
} 