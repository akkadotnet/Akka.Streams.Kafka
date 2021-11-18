using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class PlainPartitionedSourceIntegrationTests : KafkaIntegrationTests
    {
        public PlainPartitionedSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(PlainPartitionedSourceIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task PlainPartitionedSource_should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;
            var receivedMessages = new AtomicCounter(0); 

            var consumerSettings = CreateConsumerSettings<string>(group);

            var control = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .GroupBy(3, tuple => tuple.Item1)
                .SelectAsync(8, async tuple =>
                {
                    var (topicPartition, source) = tuple;
                    Log.Info($"Sub-source for {topicPartition}");
                    var sourceMessages = await source
                        .Scan(0, (i, message) => i + 1)
                        .Select(i =>
                        {
                            receivedMessages.IncrementAndGet();
                            return LogReceivedMessages(topicPartition, i);
                        })
                        .RunWith(Sink.Last<long>(), Materializer);

                    Log.Info($"{topicPartition}: Received {sourceMessages} messages in total");
                    return sourceMessages;
                })
                .MergeSubstreams()
                .As<Source<long, IControl>>()
                .Scan(0L, (i, subValue) => i + subValue)
                .ToMaterialized(Sink.Last<long>(), Keep.Both)
                .MapMaterializedValue(tuple => DrainingControl<long>.Create(tuple.Item1, tuple.Item2))
                .Run(Materializer);
            
            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);

            for (var i = 0; i < totalMessages; ++i)
                await AwaitConditionAsync(() => receivedMessages.Current > i, TimeSpan.FromSeconds(10));

            await Task.Delay(1000); // Wait for message handling finished after all messages received

            var shutdown = control.DrainAndShutdown();
            AwaitCondition(() => shutdown.IsCompleted, TimeSpan.FromSeconds(10));
            shutdown.Result.Should().Be(totalMessages);
        }

        [Fact]
        public async Task PlainPartitionedSource_Should_split_messages_by_partitions()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;

            var consumerSettings = CreateConsumerSettings<string>(group);

            var control = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .SelectAsync(6, async tuple =>
                {
                    var (topicPartition, source) = tuple;
                    Log.Info($"Sub-source for {topicPartition}");
                    var consumedPartitions = await source
                        .Select(m => m.TopicPartition.Partition)
                        .RunWith(Sink.Seq<Partition>(), Materializer);

                    // Return flag that all messages in child source are from the same, expected partition 
                    return consumedPartitions.All(partition => partition == topicPartition.Partition);
                })
                .As<Source<bool, IControl>>()
                .ToMaterialized(Sink.Aggregate<bool, bool>(true, (result, childSourceIsValid) => result && childSourceIsValid), Keep.Both)
                .MapMaterializedValue(tuple => DrainingControl<bool>.Create(tuple.Item1, tuple.Item2))
                .Run(Materializer);
            
            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);

            // Give it some time to consume all messages
            await Task.Delay(5000);

            var shutdown = control.DrainAndShutdown();
            AwaitCondition(() => shutdown.IsCompleted, TimeSpan.FromSeconds(10));
            shutdown.Result.Should().BeTrue();
        }

        [Fact]
        public async Task PlainPartitionedSource_should_stop_partition_sources_when_stopped()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;
            
            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group).WithStopTimeout(TimeSpan.FromMilliseconds(10));
            var (control, probe) = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .MergeMany(3, tuple => tuple.Item2.MapMaterializedValue(notUsed => new NoopControl()))
                .Select(message =>
                {
                    Log.Debug($"Consumed partition {message.Partition.Value}");
                    return message.Value;
                })
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);

            probe.Request(totalMessages).Within(TimeSpan.FromSeconds(10), () => probe.ExpectNextN(totalMessages));
                    
            var stopped = control.Stop();
            probe.ExpectComplete();
            
            AwaitCondition(() => stopped.IsCompleted, TimeSpan.FromSeconds(10));

            await control.Shutdown();
            probe.Cancel();
        }

        [Fact]
        public async Task PlainPartitionedSource_should_be_signalled_the_stream_by_partitioned_sources()
        {
            var settings = CreateConsumerSettings<string>(CreateGroup(1))
                .WithBootstrapServers("localhost:1111"); // Bad address

            var result = KafkaConsumer.PlainPartitionedSource(settings, Subscriptions.Topics("topic"))
                .RunWith(Sink.First<(TopicPartition, Source<ConsumeResult<Null, string>, NotUsed>)>(), Materializer);

            result.Invoking(r => r.Wait()).Should().Throw<KafkaException>();
        }
         
        [Fact]
        public async Task PlainPartitionedSource_should_be_signalled_about_serialization_errors()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            
            await ProduceStrings(topic, new int[] { 0 }, ProducerSettings); // Produce "0" string

            var settings = CreateConsumerSettings<int>(group).WithValueDeserializer(Deserializers.Int32);
            
            var (control1, partitionedProbe) = KafkaConsumer.PlainPartitionedSource(settings, Subscriptions.Topics(topic))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.StoppingDecider))
                .ToMaterialized(this.SinkProbe<(TopicPartition, Source<ConsumeResult<Null, int>, NotUsed>)>(), Keep.Both)
                .Run(Materializer);

            partitionedProbe.Request(3);
            
            var subsources = partitionedProbe.Within(TimeSpan.FromSeconds(10), () => partitionedProbe.ExpectNextN(3).Select(t => t.Item2).ToList());
            var substream = subsources.Aggregate((s1, s2) => s1.Merge(s2)).RunWith(this.SinkProbe<ConsumeResult<Null, int>>(), Materializer);

            substream.Request(1);
            
            await ProduceStrings(topic, new int[] { 0 }, ProducerSettings); // Produce "0" string

            Within(TimeSpan.FromSeconds(10), () =>
            {
                var err = substream.ExpectError();
                err.Should().BeOfType<ConsumeException>();
                ((ConsumeException) err).Error.IsSerializationError().Should().BeTrue();
            });

            var shutdown = control1.Shutdown();
            AwaitCondition(() => shutdown.IsCompleted, TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task PlainPartitionedSource_should_not_leave_gaps_when_subsource_is_cancelled()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 100;

            await ProduceStrings(topic, Enumerable.Range(1, totalMessages), ProducerSettings);
            
            var consumedMessagesTask = KafkaConsumer.PlainPartitionedSource(CreateConsumerSettings<string>(group), Subscriptions.Topics(topic))
                .Log(topic, m => $"Consuming topic partition {m.Item1}")
                .MergeMany(3, tuple =>
                {
                    var (topicPartition, source) = tuple;
                    return source
                        .MapMaterializedValue(notUsed => new NoopControl())
                        .Log(topicPartition.ToString(), m => $"Consumed offset {m.Offset} (value: {m.Value})")
                        .Take(10);
                })
                .Select(m => int.Parse(m.Value))
                .Log("Merged stream", m => m)
                .Scan(0, (c, _) => c + 1)
                .TakeWhile(m => m < totalMessages, inclusive: true)
                .RunWith(Sink.Last<int>(), Materializer);
            
            AwaitCondition(() => consumedMessagesTask.IsCompleted, TimeSpan.FromSeconds(10));

            consumedMessagesTask.Result.Should().Be(totalMessages);
        }

        [Fact]
        public async Task PlainPartitionedSource_should_not_leave_gaps_when_subsource_failed()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var totalMessages = 105;

            var producerSettings = BuildProducerSettings<string, string>();
            
            await Source
                .From(Enumerable.Range(1, totalMessages))
                .Select(elem => new ProducerRecord<string, string>(topic, elem.ToString(), elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), Materializer);

            var queue = new ConcurrentQueue<int>();
            
            var consumerSettings = ConsumerSettings<string, string>.Create(Sys, null, null)
                .WithBootstrapServers(Fixture.KafkaServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
            
            KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .RunForeach(tuple =>
                {
                    var (topicPartition, source) = tuple;
                    
                    Log.Info($"Consuming topic partition {topicPartition}");
                    source
                        .Select(message =>
                        {
                            var value = int.Parse(message.Message.Value);
                            queue.Enqueue(value);
                            
                            if (value % 10 == 0)
                            {
                                Log.Debug("[{0}] Reached message to fail: offset: [{1}], value: [{2}]", message.TopicPartition, message.Offset, value);
                                throw new Exception("Stopping subsource");
                            }

                            return value;
                        })
                        .RunWith(Sink.Ignore<int>(), Materializer);
                }, Materializer);

            await AwaitConditionAsync(
                () => queue.Count >= totalMessages, 
                TimeSpan.FromSeconds(60),
                TimeSpan.FromMilliseconds(100));

            var sorted = queue.ToImmutableSortedSet();
            sorted.Should().BeEquivalentTo(Enumerable.Range(1, totalMessages));
        }

        // This is a rough benchmark number for the unit test above, of how much time it should have taken
        // if it is implemented using native client and given the best running environment as possible
        // (no threading, no stream, etc).
        [Fact(DisplayName = "Native Kafka IConsumer.Seek() should not fail to seek to proper offset")]
        public void NativeKafkaIConsumerSeekShouldNotFail()
        {
            var topic = "topic-1";
            var group = "group-1";
            var totalMessages = 105;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = Fixture.KafkaServer
            };

            var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            foreach (var i in Enumerable.Range(1, totalMessages))
            {
                producer.Produce(topic, new Message<string, string>
                {
                    Key = i.ToString(),
                    Value = i.ToString()
                });
            }

            producer.Flush();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = Fixture.KafkaServer,
                GroupId = group,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .Build();
            consumer.Subscribe(topic);
            
            const int consumeTimeout = 50;
            const int testTimeout = 10_000;
            
            var consumeCount = 0;

            var offsets = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset(new TopicPartition(topic, 0), 0),
                new TopicPartitionOffset(new TopicPartition(topic, 1), 0),
                new TopicPartitionOffset(new TopicPartition(topic, 2), 0),
            };
            
            var watch = Stopwatch.StartNew();
            while (consumeCount < totalMessages && watch.ElapsedMilliseconds < testTimeout)
            {
                var consumed = ConsumeAllMessages(consumer, totalMessages, offsets, consumeTimeout);
                //Log.Info($"Polled {consumed.consumed} messages");
                
                if (consumed.consumed != 0)
                {
                    for(var i=0; i<3; i++)
                    {
                        var seekResult = CheckForSeek(consumed.messages[i]);
                        consumeCount += seekResult.count;
                        offsets[i] = seekResult.seekOffset;
                    }

                    foreach (var offset in offsets)
                    {
                        if(offset != null)
                        {
                            Log.Info($"Seeking {offset.TopicPartition} to {offset.Offset}");
                            consumer.Seek(offset);
                        }
                    }
                }
                
                Thread.Sleep(consumeTimeout);
            }
            watch.Stop();

            consumeCount.Should().Be(totalMessages);
        }

        private (int count, TopicPartitionOffset seekOffset) CheckForSeek(List<ConsumeResult<string, string>> messages)
        {
            var messageCount = 0;
            foreach (var msg  in messages)
            {
                messageCount++;
                if (int.TryParse(msg.Message.Value, out var i) && i % 10 == 0)
                    return (messageCount, new TopicPartitionOffset(msg.TopicPartition, msg.Offset + 1));
            }

            return (messageCount, null);
        }

        private (int consumed, List<ConsumeResult<string, string>>[] messages) ConsumeAllMessages(
            IConsumer<string, string> consumer,
            long totalMessages,
            List<TopicPartitionOffset> offsets,
            int consumeTimeout)
        {
            var result = new []
            {
                new List<ConsumeResult<string, string>>(),
                new List<ConsumeResult<string, string>>(),
                new List<ConsumeResult<string, string>>(),
            };

            totalMessages = offsets
                .Where(offset => offset != null)
                .Aggregate(totalMessages, (current, offset) => current - offset.Offset.Value);

            var consumeCount = 0;
            var watch = Stopwatch.StartNew();
            // poll messages
            while (consumeCount < totalMessages && watch.ElapsedMilliseconds < consumeTimeout)
            {
                var consumed = consumer.Consume(consumeTimeout);
                if (consumed != null)
                {
                    result[consumed.Partition].Add(consumed);
                    consumeCount++;
                }
            }
            watch.Stop();

            return (consumeCount, result);
        }
        
        private long LogReceivedMessages(TopicPartition tp, int counter)
        {
            if (counter % 1000 == 0)
                Log.Info($"{tp}: Received {counter} messages so far.");

            return counter;
        }
    }
}