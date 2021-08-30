using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Benchmark
{
    [MinWarmupCount(5)]
    [InvocationCount(2000)]
    [IterationCount(80)]
    public class PlainSinkConsumerBenchmark : BenchmarkBase
    {
        private IControl _kafkaControl;
        private ISinkQueue<ConsumeResult<Null, string>> _sink;
        
        [GlobalSetup(Target = nameof(PlainSinkThroughput))]
        public async Task GlobalSetupAkkaAsync()
        {
            await SetupKafkaAsync();
            await SetupAkkaAsync();
            
            StartProducer();
            
            var consumerSettings = ConsumerSettings<Null, string>
                .Create(ConsumerSystem, null, null)
                .WithBootstrapServers(Docker.KafkaAddress)
                .WithGroupId(KafkaGroup);
            
            var (control, queue) = KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(KafkaTopic))
                .ToMaterialized(
                    Sink.Queue<ConsumeResult<Null, string>>()
                        .AddAttributes(new Attributes(new Attributes.InputBuffer(2000, 4000))), 
                    Keep.Both)
                .Run(ConsumerSystem.Materializer());

            _kafkaControl = control;
            _sink = queue;
        }

        [Benchmark]
        public async Task PlainSinkThroughput()
        {
            var result = await _sink.PullAsync();
            if(!result.HasValue)
                Console.WriteLine("Consume timed out.");
        }
        
        [GlobalCleanup(Target = nameof(PlainSinkThroughput))]
        public async Task CleanupAkkaAsync()
        {
            if (_producerThread != null)
            {
                _done = true;
                _producerThread.Join(1000);
            }

            await _kafkaControl.Shutdown();
            await TearDownAkkaAsync();
            await TearDownKafkaAsync();
        }

        private Thread _producerThread;
        private bool _done;
        private IConsumer<Null, string> _consumer;
        [GlobalSetup(Target = nameof(KafkaClientThroughput))]
        public async Task GlobalSetupKafkaAsync()
        {
            await SetupKafkaAsync();

            StartProducer();
            
            var clientConfig = new ClientConfig
            {
                BootstrapServers = Docker.KafkaAddress
            };

            var consumerConfig = new ConsumerConfig(clientConfig)
            {
                AutoOffsetReset = AutoOffsetReset.Earliest, 
                EnableAutoCommit = true, 
                GroupId = KafkaGroup
            };

            var builder = new ConsumerBuilder<Null, string>(consumerConfig);
            _consumer = builder.Build();
            _consumer.Subscribe(KafkaTopic);
        }
        
        [Benchmark]
        public void KafkaClientThroughput()
        {
            var result = _consumer.Consume(1000);
            if(result == null)
                Console.WriteLine("Consume timed out.");
        }

        [GlobalCleanup(Target = nameof(KafkaClientThroughput))]
        public async Task CleanupKafkaAsync()
        {
            _consumer?.Dispose();
            if (_producerThread != null)
            {
                _done = true;
                _producerThread.Join(1000);
            }

            await TearDownKafkaAsync();
        }

        private void StartProducer()
        {
            _producerThread = new Thread(() =>
            {
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = Docker.KafkaAddress,
                    LingerMs = 100,
                    BatchSize = 500
                };

                var counter = 0;
                var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
                while (!_done)
                {
                    for (var i = 0; i < 800; ++i)
                    {
                        producer.Produce(KafkaTopic, new Message<Null, string> {Value = counter.ToString()});
                        counter++;
                    }
                    Thread.Sleep(100);
                }
            });
            _producerThread.Start();
        }
    }
}