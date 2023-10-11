﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.TestKit;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Google.Protobuf;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests
{
    public class BugFixRestartSourceSpec: KafkaIntegrationTests
    {
        private readonly Random _rnd = new Random(123456);
        
        public BugFixRestartSourceSpec(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(BugFixRestartSourceSpec), output, fixture)
        {
        }

        // Note that this is NOT a spec, this is meant to hammer CommittablePartitionedSource and find possible bugs  
        [Fact(Skip = "Could not assert this while the underlying stream is suppressing failures")]
        public async Task KafkaSourceShouldNotLeakActorsWhenRestartedUsingRestartSource()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);

            // Test setup: create topic and continuously generate messages
            await GivenInitializedTopic(topic);
            var produceTask = ProduceContinually(topic, 100, 300, TimeSpan.FromSeconds(5));

            // Test setup: create settings instances
            // Original issue post have these Kafka client settings set up
            /*
            var properties = new Dictionary<string, string>();
            properties.Add("auto.offset.reset", "earliest");
            properties.Add("session.timeout.ms", "60000");
            properties.Add("max.poll.interval.ms", "60000");
            properties.Add("socket.keepalive.enable", "true");
            properties.Add("metadata.max.age.ms", "180000");

            if (configuration["protocol"] == "SASL_SSL")
            {
                properties.Add("security.protocol", "SASL_SSL");
                properties.Add("sasl.mechanism", "PLAIN");
                properties.Add("sasl.username", "$ConnectionString");
                properties.Add("sasl.password", appConfig.EventHubConnectionString);
            }
            */

            // Original issue post have these restart settings set up
            /*
            var restartSettings = RestartSettings.Create(
                minBackoff: TimeSpan.FromSeconds(3),
                maxBackoff: TimeSpan.FromSeconds(30),
                randomFactor: 0.2
            ).WithMaxRestarts(20, TimeSpan.FromMinutes(5));
            */
            var consumerSettings = CreateConsumerSettings<string>(group).WithStopTimeout(TimeSpan.FromSeconds(2));
            var committerSettings = CommitterSettings.Create(Sys);
            var restartSettings = RestartSettings.Create(
                minBackoff: TimeSpan.FromSeconds(1),
                maxBackoff: TimeSpan.FromSeconds(3),
                randomFactor: 0.02
            ).WithMaxRestarts(20, TimeSpan.FromMinutes(5));

            // Start Kafka partitioned source wrapped inside a RestartSource
            var completed = new AtomicCounter(0);
            var failCounter = new AtomicCounter();
            var source = RestartSource.OnFailuresWithBackoff(() =>
            {
                Log.Info("Building Kafka consumer");

                return KafkaConsumer.CommittablePartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                    .GroupBy(KafkaFixture.KafkaPartitions, tuple => tuple.Item1)
                    .SelectAsync(10, async tuple =>
                    {
                        var (topicPartition, source) = tuple;
                        Log.Info($"{topicPartition}: Sub-source started");

                        var sourceMessages = await source
                            .SelectAsync(5, async i =>
                            {
                                // Fail every 500 messages to force RestartSource to keep restarting
                                var fail = failCounter.IncrementAndGet();
                                if(fail % 500 == 0)
                                {
                                    throw new Exception("BOOM!");
                                }
                                
                                completed.IncrementAndGet();
                                return (ICommittable) i.CommitableOffset;
                            })
                            .Via(Committer.Flow(committerSettings.WithMaxInterval(TimeSpan.FromSeconds(3))))
                            .RunWith(Sink.LastOrDefault<Done>(), Materializer);

                        Log.Info($"{topicPartition}: Sub-source completed received {sourceMessages}");

                        return sourceMessages;
                    })
                    .MergeSubstreams()
                    .AsInstanceOf<Source<Done, IControl>>();

            }, restartSettings);

            source.ToMaterialized(Sink.Ignore<Done>(), Keep.Left)
                .Run(Materializer);

            // This is very suspect, Kafka consumer actor is created under **/system** as a top level actor
            // with no guardian, which is very wrong
            Sys.ActorSelection("/system").Tell(new Identify(0), TestActor);
            var systemRef = (ActorRefWithCell) ExpectMsg<ActorIdentity>().Subject;

            var lastComplete = 0L;
            foreach(var _ in Enumerable.Range(0, 50))
            {
                await Task.Delay(TimeSpan.FromSeconds(10));

                var found = 0;
                var cell = systemRef.Underlying;
                foreach (var child in cell.ChildrenContainer.Children)
                {
                    if (child.Path.ToString().Contains("kafka-consumer"))
                        found++;
                }

                var complete = completed.Current;
                Log.Warning($"Processed messages: [{complete}]");
                Log.Warning($"Kafka consumer actors: [{found}]");
                found.Should().BeLessOrEqualTo(3);

                if (complete != 0 && complete == lastComplete)
                    break;
                
                lastComplete = complete;
            }
        }

        [Fact]
        public async Task KafkaSourceShouldNotLeakActorsWhenStageFailedWithAnException()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);

            await GivenInitializedTopic(topic);
            await ProduceStrings(
                i => new TopicPartition(topic, i % KafkaFixture.KafkaPartitions), 
                Enumerable.Range(1, 600),
                ProducerSettings);

            var consumerSettings = CreateConsumerSettings<string>(group).WithStopTimeout(TimeSpan.FromSeconds(2));
            var committerSettings = CommitterSettings.Create(Sys);

            // Start Kafka partitioned source wrapped inside a RestartSource
            var completed = new AtomicCounterLong(0);
            var failCounter = new AtomicCounter();
            var failCount = new AtomicCounter(0);
            var source = KafkaConsumer.CommittablePartitionedSource(consumerSettings, Subscriptions.Topics(topic))
                .GroupBy(KafkaFixture.KafkaPartitions, tuple => tuple.Item1)
                .SelectAsync(3, async tuple =>
                {
                    var (topicPartition, source) = tuple;

                    var sourceMessages = await source
                        .WithAttributes(new Attributes(new Attributes.LogLevels(LogLevel.InfoLevel, LogLevel.InfoLevel, LogLevel.ErrorLevel)))
                        .Log($"{topicPartition}: Sub-source started")
                        .Select(message =>
                        {
                            // Fail at message 50
                            var fail = failCounter.IncrementAndGet();
                            if(fail % 50 == 0)
                            {
                                failCount.IncrementAndGet();
                                throw new Exception("BOOM!");
                            }
                            
                            completed.IncrementAndGet();
                            return message;
                        })
                        .SelectAsync(5, async message =>
                        {
                            try
                            {
                                await message.CommitableOffset.Commit();
                            }
                            catch (Exception ex)
                            {
                                Log.Error(ex, "Commit failure");
                            }

                            return message;
                        })
                        .Scan(0, (c, _) => c + 1)
                        .RunWith(Sink.Last<int>(), Materializer);
                    
                    Log.Info($"{topicPartition}: Sub-source completed");

                    return sourceMessages;
                })
                .MergeSubstreams()
                .AsInstanceOf<Source<int, IControl>>();

            var (control, last) = source.ToMaterialized(Sink.Last<int>(), Keep.Both)
                .Run(Materializer);

            await AwaitConditionAsync(() => failCount.Current > 0, TimeSpan.FromSeconds(10));
            Log.Info($"Stream should be dead at this point. Fail count: [{failCount.Current}]");
            
            Sys.ActorSelection("/system").Tell(new Identify(0), TestActor);
            var systemRef = (ActorRefWithCell) ExpectMsg<ActorIdentity>().Subject;

            var lastFail = 1;
            await AwaitAssertAsync(() =>
            {
                var fail = failCount.Current;
                if(fail != lastFail)
                {
                    Log.Info($"Fail count: {fail}");
                    lastFail = fail;
                }
                
                var found = 0;
                var cell = systemRef.Underlying;
                foreach (var child in cell.ChildrenContainer.Children)
                {
                    if (child.Path.ToString().Contains("kafka-consumer"))
                        found++;
                }
                found.Should().Be(0);
            }, TimeSpan.FromSeconds(30));
        }
        
        private int _lastMsg = 1;
        private async Task ProduceContinually(string topic, int minMsg, int maxMsg, TimeSpan delay)
        {
            foreach (var _ in Enumerable.Range(0, 1000))
            {
                var msgCount = _rnd.Next(minMsg, maxMsg);
                await ProduceStrings(
                    i => new TopicPartition(topic, i % KafkaFixture.KafkaPartitions), 
                    Enumerable.Range(_lastMsg, msgCount),
                    ProducerSettings);
                _lastMsg += msgCount;
                await Task.Delay(delay);
            }
        }
    }
}