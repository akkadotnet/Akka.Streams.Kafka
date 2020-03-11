using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class CommittablePartitionedSourceIntegrationTests : KafkaIntegrationTests
    {
        public CommittablePartitionedSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(CommittablePartitionedSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task CommittablePartitionedSource_Should_handle_exceptions_in_stream_without_commit_failures()
        {
            var partitionsCount = 3;
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;
            var exceptionTriggered = new AtomicBoolean(false);
            var allTopicPartitions = Enumerable.Range(0, partitionsCount).Select(i => new TopicPartition(topic, i)).ToList();

            var consumerSettings = CreateConsumerSettings<string>(group).WithStopTimeout(TimeSpan.FromSeconds(2));

            var createdSubSources = new ConcurrentSet<TopicPartition>();
            var commitFailures = new ConcurrentSet<(TopicPartition, Exception)>();
            
            var control = KafkaConsumer.CommittablePartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .GroupBy(partitionsCount, tuple => tuple.Item1)
                .SelectAsync(6, tuple =>
                {
                    var (topicPartition, source) = tuple;
                    createdSubSources.TryAdd(topicPartition);
                    return source
                        .Log($"Subsource for partition #{topicPartition.Partition.Value}", m => m.Record.Value)
                        .SelectAsync(3, async message =>
                        {
                            // fail on first partition; otherwise delay slightly and emit
                            if (topicPartition.Partition.Value == 0)
                            {
                                Log.Debug($"Failing {topicPartition} source");
                                exceptionTriggered.GetAndSet(true);
                                throw new Exception("FAIL");
                            }
                            else
                            {
                                await Task.Delay(50);
                            }

                            return message;
                        })
                        .Log($"Subsource {topicPartition} pre commit")
                        .SelectAsync(1, async message =>
                        {
                            try
                            {
                                await message.CommitableOffset.Commit();
                            }
                            catch (Exception ex)
                            {
                                Log.Error("Commit failure: " + ex);
                                commitFailures.TryAdd((topicPartition, ex));
                            }

                            return message;
                        })
                        .Scan(0, (c, _) => c + 1)
                        .RunWith(Sink.Last<int>(), Materializer)
                        .ContinueWith(t =>
                        {
                            Log.Info($"sub-source for {topicPartition} completed: Received {t.Result} messages in total.");
                            return t.Result;
                        });
                })
                .MergeSubstreams()
                .As<Source<int, IControl>>()
                .Scan(0, (c, n) => c + n)
                .ToMaterialized(Sink.Last<int>(), Keep.Both)
                .MapMaterializedValue(tuple => DrainingControl<int>.Create(tuple.Item1, tuple.Item2))
                .Run(Materializer);
            
            await ProduceStrings(i => new TopicPartition(topic, i % partitionsCount), Enumerable.Range(1, totalMessages), ProducerSettings);
            
            AwaitCondition(() => exceptionTriggered.Value, TimeSpan.FromSeconds(10));

            var shutdown = control.DrainAndShutdown();
            AwaitCondition(() => shutdown.IsCompleted);
            createdSubSources.Should().Contain(allTopicPartitions);
            shutdown.Exception.GetBaseException().Message.Should().Be("FAIL");

            // commits will fail if we shut down the consumer too early
            commitFailures.Should().BeEmpty();

        }
    }
}