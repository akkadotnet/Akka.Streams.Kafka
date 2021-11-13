using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Tests.TestKit.Internal;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Tests.Internal
{
    using K = String;
    using V = String;
    using Record = ConsumeResult<string, string>;

    public class ConsumerSpec: Akka.TestKit.Xunit2.TestKit
    {
        private static CommittableMessage<K, V> CreateMessage(int seed)
            => CreateMessage(seed, "topic");
    
        private static CommittableMessage<K, V> CreateMessage(
            int seed,
            string topic,
            string groupId = "group1",
            string metadata = "")
        {
            var offset = new GroupTopicPartitionOffset(new GroupTopicPartition(groupId, topic, 1), seed);
            var record = new Record
            {
                Topic = offset.Topic,
                Partition = offset.Partition,
                Offset = offset.Offset,
                Message = new Message<string, string>
                {
                    Key = seed.ToString(),
                    Value = seed.ToString()
                }
            };
            return new CommittableMessage<string, string>(
                record,
                new CommittableOffset(ConsumerResultFactory.FakeCommiter, offset, metadata));
        }

        private static Record ToRecord(CommittableMessage<K, V> msg)
            => msg.Record;

        private static readonly Config Config =
            ConfigurationFactory.ParseString(@"
akka.loglevel = DEBUG
akka.stream.materializer.debug.fuzzing-mode = on")
                .WithFallback(KafkaExtensions.DefaultSettings);
    
        public ConsumerSpec(ITestOutputHelper output) 
            : base(Config, nameof(ConsumerSpec), output)
        { }

        private readonly ImmutableList<CommittableMessage<K, V>> Messages =
            Enumerable.Range(1, 1000).Select(CreateMessage).ToImmutableList();

        private async Task CheckMessagesReceiving(List<List<CommittableMessage<K, V>>> msgss)
        {
            var mock = new MockConsumer<K, V>();
            var (control, probe) = CreateCommitableSource(mock)
                .ToMaterialized(this.SinkProbe<CommittableMessage<K, V>>(), Keep.Both)
                .Run(Sys.Materializer());

            probe.Request(msgss.Select(t => t.Count).Sum());
            foreach (var chunk in msgss)
            {
                mock.Enqueue(chunk.Select(l => l.Record).ToList());
            }

            var messages = msgss.SelectMany(m => m).Select(m => m);
            foreach (var message in messages)
            {
                var received = probe.ExpectNext();
                received.Record.Message.Key.Should().Be(message.Record.Message.Key);
                received.Record.Message.Value.Should().Be(message.Record.Message.Value);
            }
            await GuardTimeout(control.Shutdown(), RemainingOrDefault);
        }

        private Source<CommittableMessage<K, V>, IControl> CreateCommitableSource(
            MockConsumer<K, V> mock, string groupId = "group1", string[] topics = null)
        {
            topics ??= new[] {"topic"};
            var settings = ConsumerSettings<K, V>.Create(Sys, Deserializers.Utf8, Deserializers.Utf8)
                .WithGroupId(groupId)
                .WithCloseTimeout(MockConsumer.CloseTimeout)
                .WithStopTimeout(MockConsumer.CloseTimeout)
                .WithCommitTimeout(TimeSpan.FromMilliseconds(500))
                .WithConsumerFactory(_ => mock.Mock);
            mock.Settings = settings;
            
            return KafkaConsumer.CommittableSource(
                settings,
                Subscriptions.Topics(topics));
        }

        private Source<CommittableMessage<K, V>, IControl> CreateSourceWithMetadata(
            MockConsumer<K, V> mock,
            Func<ConsumeResult<K, V>, string> metadataFromRecord,
            string groupId = "group1",
            string[] topics = null)
        {
            var settings = ConsumerSettings<K, V>.Create(Sys, Deserializers.Utf8, Deserializers.Utf8)
                .WithGroupId(groupId)
                .WithConsumerFactory(_ => mock.Mock);
            mock.Settings = settings;
            
            return KafkaConsumer.CommitWithMetadataSource(
                settings,
                Subscriptions.Topics(topics),
                metadataFromRecord);
        }

        [Fact(DisplayName = "CommittableSource should fail stream when poll() fails with unhandled exception")]
        public void ShouldFailWhenPollFails()
        {
            var mock = new FailingMockConsumer<K, V>(new Exception("Fatal Kafka error"), 1);
            var probe = CreateCommitableSource(mock)
                .ToMaterialized(this.SinkProbe<CommittableMessage<K, V>>(), Keep.Right)
                .Run(Sys.Materializer());

            probe.Request(1).ExpectError();
        }

        [Fact(DisplayName = "CommittableSource should complete stage when stream control.stop called")]
        public async Task ShouldCompleteWhenStopped()
        {
            var mock = new MockConsumer<K, V>();
            var (control, probe) = CreateCommitableSource(mock)
                .ToMaterialized(this.SinkProbe<CommittableMessage<K, V>>(), Keep.Both)
                .Run(Sys.Materializer());

            probe.Request(100);

            await GuardTimeout(control.Shutdown(), TimeSpan.FromSeconds(10));
            probe.ExpectComplete();
            mock.VerifyClosed();
        }

        [Fact(DisplayName = "CommittableSource should complete stage when processing flow canceled")]
        public async Task ShouldCompleteWhenCanceled()
        {
            var mock = new MockConsumer<K, V>();
            var (control, probe) = CreateCommitableSource(mock)
                .ToMaterialized(this.SinkProbe<CommittableMessage<K, V>>(), Keep.Both)
                .Run(Sys.Materializer());

            probe.Request(100);
            mock.VerifyNotClosed();
            probe.Cancel();
            await GuardTimeout(control.IsShutdown, RemainingOrDefault);
            mock.VerifyClosed();
        }

        [Fact(DisplayName = "CommittableSource should emit messages received as one big chunk")]
        public async Task ShouldEmitBigChunk()
        {
            await CheckMessagesReceiving(
                new List<List<CommittableMessage<string, string>>> { Messages.ToList() } );
        }
        
        [Fact(DisplayName = "CommittableSource should emit messages received as medium chunk")]
        public async Task ShouldEmitMediumChunk()
        {
            var splits = new List<List<CommittableMessage<string, string>>>();
            var list = new List<CommittableMessage<string, string>>();
            for (var i = 0; i < splits.Count; ++i)
            {
                list.Add(Messages[i]);
                if(i != 0 && i % 97 == 0)
                {
                    splits.Add(list);
                    list = new List<CommittableMessage<string, string>>();
                }
            }
            await CheckMessagesReceiving(splits);
        }
        
        [Fact(DisplayName = "CommittableSource should emit messages received as chunked singles")]
        public async Task ShouldEmitSingles()
        {
            var splits = new List<List<CommittableMessage<string, string>>>();
            foreach (var message in Messages)
            {
                splits.Add(new List<CommittableMessage<string, string>>{message});
            }
            await CheckMessagesReceiving(splits);
        }
        
        private async Task GuardTimeout(Task task, TimeSpan timeout)
        {
            var cts = new CancellationTokenSource();
            try
            {
                var timeoutTask = Task.Delay(timeout, cts.Token);
                var completed = await Task.WhenAny(task, timeoutTask);
                if (completed == timeoutTask)
                    throw new OperationCanceledException("Operation timed out");
                else
                    cts.Cancel();
            }
            finally
            {
                cts.Dispose();
            }
        }
    }
}