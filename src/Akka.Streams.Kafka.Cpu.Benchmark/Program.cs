
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Event;
using Akka.Streams.Kafka.Messages;
using Akka.Util.Internal;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Universe.CpuUsage;
using static Akka.Streams.Kafka.Cpu.Benchmark.Benchmark;

namespace Akka.Streams.Kafka.Cpu.Benchmark
{
    public static class Program
    {
        private const int DefaultSampleDuration = 5; // in seconds
        private const int DefaultDelay = 5; // in seconds
        private const int DefaultRepeat = 20;
        private const int DefaultMessageCount = 1000;
        private const int DefaultTimeout = 30;

        private const int DefaultWarmUpRepeat = 5;
        
        private static ILoggingAdapter _log;
        private static readonly AtomicCounter ReceivedMessage = new AtomicCounter(0);
        
        public static async Task<int> Main(string[] args)
        {
            // Setup
            await SetupKafkaAsync();
            await SetupAkkaAsync();

            List<CpuUsage> usageBeforeLoad;
            List<CpuUsage> usageAfterLoad;
            
            try
            {
                _log = Logging.GetLogger(ConsumerSystem, nameof(Program));
                
                // Create topic on Kafka server
                var builder = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = Benchmark.Docker.KafkaAddress
                });
                using (var client = builder.Build())
                {
                    await client.CreateTopicsAsync(new[] {new TopicSpecification
                    {
                        Name = KafkaTopic,
                        NumPartitions = 3,
                        ReplicationFactor = 1
                    }});
                }
                
                // Set up consumer
                var consumerSettings = ConsumerSettings<string, string>.Create(ConsumerSystem, null, null)
                    .WithBootstrapServers(Benchmark.Docker.KafkaAddress)
                    .WithStopTimeout(TimeSpan.FromSeconds(1))
                    .WithProperty("auto.offset.reset", "earliest")
                    .WithGroupId(KafkaGroup);
                
                var control = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(KafkaTopic))
                    .GroupBy(3, tuple => tuple.Item1)
                    .SelectAsync(8, async tuple =>
                    {
                        var (topicPartition, source) = tuple;
                        _log.Info($"Sub-source for {topicPartition}");
                        var sourceMessages = await source
                            .Scan(0, (i, message) => i + 1)
                            .Select(i =>
                            {
                                ReceivedMessage.IncrementAndGet();
                                return LogReceivedMessages(topicPartition, i);
                            })
                            .RunWith(Sink.Last<long>(), ConsumerSystem.Materializer());

                        _log.Info($"{topicPartition}: Received {sourceMessages} messages in total");
                        return sourceMessages;
                    })
                    .MergeSubstreams()
                    .AsInstanceOf<Source<long, IControl>>()
                    .Scan(0L, (i, subValue) => i + subValue)
                    .ToMaterialized(Sink.Last<long>(), Keep.Both)
                    .MapMaterializedValue(tuple => DrainingControl<long>.Create(tuple.Item1, tuple.Item2))
                    .Run(ConsumerSystem.Materializer());
                
                // Delay before benchmark
                await Task.Delay(TimeSpan.FromSeconds(DefaultDelay));
                
                // Warmup
                await CollectSamplesAsync(DefaultWarmUpRepeat, DefaultSampleDuration, "[Warmup]");
                
                // Collect CPU usage before load
                usageBeforeLoad = await CollectSamplesAsync(DefaultRepeat, DefaultSampleDuration, "[CPU Usage Before Load]");
                
                // Create load
                var producerSettings = ProducerSettings<string, string>.Create(ConsumerSystem, null, null)
                    .WithBootstrapServers(Benchmark.Docker.KafkaAddress);
                
                await Source
                    .From(Enumerable.Range(1, DefaultMessageCount))
                    .Select(elem => new ProducerRecord<string, string>(KafkaTopic, "key", elem.ToString()))
                    .RunWith(KafkaProducer.PlainSink(producerSettings), ConsumerSystem.Materializer());

                // Wait until consumer consumed all messages
                var stopwatch = Stopwatch.StartNew();
                while (stopwatch.Elapsed.TotalSeconds < DefaultTimeout && ReceivedMessage.Current < DefaultMessageCount)
                {
                    await Task.Delay(100);
                }
                stopwatch.Stop();
                if (stopwatch.Elapsed.TotalSeconds > DefaultTimeout)
                {
                    throw new Exception($"Timed out while waiting consumer to process {DefaultMessageCount} messages");
                }
                
                // Delay before benchmark
                await Task.Delay(TimeSpan.FromSeconds(DefaultDelay));
                
                // Collect CPU usage after load
                usageAfterLoad = await CollectSamplesAsync(DefaultRepeat, DefaultSampleDuration, "[CPU Usage After Load]");
            }
            finally
            {
                // Tear down
                await TearDownAkkaAsync();
                await TearDownKafkaAsync();
            }
            
            Console.WriteLine("CPU Benchmark complete.");
            await GenerateReportAsync(usageBeforeLoad, "BeforeLoad", DefaultSampleDuration, DefaultRepeat);
            await GenerateReportAsync(usageAfterLoad, "AfterLoad", DefaultSampleDuration, DefaultRepeat);
            
            return 0;
        }

        private static async Task GenerateReportAsync(List<CpuUsage> usages, string type, float sampleDuration, int repeat)
        {
            // Generate csv report
            var now = DateTime.Now;
            var sb = new StringBuilder();
            sb.AppendLine($"CPU Benchmark {now}");
            sb.AppendLine($"Sample Time,{sampleDuration},second(s)");
            sb.AppendLine($"Sample points,{repeat}");
            sb.AppendLine("Sample time,User usage,User percent,Kernel usage,Kernel percent,Total usage,Total percent");
            foreach (var iter in Enumerable.Range(1, repeat))
            {
                var usage = usages[iter - 1];
                var user = usage.UserUsage.TotalSeconds;
                var kernel = usage.KernelUsage.TotalSeconds;
                var total = usage.TotalMicroSeconds / 1000000.0;
                sb.AppendLine($"{iter * sampleDuration},{user},{(user/sampleDuration)*100},{kernel},{(kernel/sampleDuration)*100},{total},{(total/sampleDuration)*100}");
            }
            
            await File.WriteAllTextAsync($"CpuBenchmark_{type}_{now.ToFileTime()}.csv", sb.ToString());
            
            // Generate console report
            sb.Clear();
            sb.AppendLine();

            sb.AppendLine(type)
                .AppendLine(" CPU Usage Mode | Mean | StdErr | StdDev | Median | Maximum |")
                .AppendLine("--------------- |----- |------- |------- |------- |-------- |")
                .AppendLine(CalculateResult(usages.Select(u => u.UserUsage.TotalMicroSeconds), "User"))
                .AppendLine(CalculateResult(usages.Select(u => u.KernelUsage.TotalMicroSeconds), "Kernel"))
                .AppendLine(CalculateResult(usages.Select(u => u.TotalMicroSeconds), "Total"));
            
            Console.WriteLine(sb.ToString());
        }
        
        private static string CalculateResult(IEnumerable<long> values, string name)
        {
            var times = values.OrderBy(i => i).ToArray();
            var medianIndex = times.Length / 2;
            
            var mean = times.Average();
            var stdDev = Math.Sqrt(times.Average(v => Math.Pow(v - mean, 2)));
            var stdErr = stdDev / Math.Sqrt(times.Length);
            double median;
            if (times.Length % 2 == 0)
                median = (times[medianIndex - 1] + times[medianIndex]) / 2.0;
            else
                median = times[medianIndex];

            return $" {name} | {(mean / 1000.0):N3} ms | {(stdErr / 1000.0):N3} ms | {(stdDev / 1000.0):N3} ms | {(median / 1000.0):N3} ms | {(times.Last() / 1000.0):N3} ms |";
        }

        private static async Task<List<CpuUsage>> CollectSamplesAsync(int repeat, float duration, string msg)
        {
            var usages = new List<CpuUsage>();
            
            foreach (var i in Enumerable.Range(1, repeat))
            {
                var start = CpuUsage.GetByProcess();
                await Task.Delay(TimeSpan.FromSeconds(duration));
                var end = CpuUsage.GetByProcess();
                var final = end - start;
                
                Console.WriteLine($"{i}. {msg}: {final}");
                usages.Add(final.Value);
            }

            return usages;
        }

        private static long LogReceivedMessages(TopicPartition tp, int counter)
        {
            if (counter % 1000 == 0)
                _log.Info($"{tp}: Received {counter} messages so far.");

            return counter;
        }
        
    }

}
