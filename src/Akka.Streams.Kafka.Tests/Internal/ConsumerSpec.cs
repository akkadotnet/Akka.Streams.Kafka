using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Tests.TestKit.Internal;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

using K = System.String;
using V = System.String;
namespace Akka.Streams.Kafka.Tests.Internal
{
    using Record = ConsumeResult<K, V>;

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
            await control.Shutdown().WithTimeout(RemainingOrDefault);
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

            await control.Shutdown().WithTimeout(TimeSpan.FromSeconds(10));
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
            await control.IsShutdown.WithTimeout(RemainingOrDefault);
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
            await CheckMessagesReceiving(Messages.Grouped(97));
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
        
        [Fact(DisplayName = "CommittableSource should emit messages received empties")]
        public async Task ShouldEmitEmpties()
        {
            await CheckMessagesReceiving(Messages.Grouped(97)
                .Select(x => new List<CommittableMessage<string, string>>()).ToList());
        }

        [Fact(DisplayName =
            "CommittableSource should complete out and keep underlying client open when control.stop called")]
        public void ShouldKeepClientOpenOnStop()
        {
            this.AssertAllStagesStopped(() =>
            {
                
            }, Sys.Materializer());
        }
    }

    internal static class Extensions
    {
        public static async Task WithTimeout(this Task task, TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource())
            {
                var timeoutTask = Task.Delay(timeout, cts.Token);
                var completed = await Task.WhenAny(task, timeoutTask);
                if (completed == timeoutTask)
                    throw new OperationCanceledException("Operation timed out");
                else
                    cts.Cancel();
            }
        }
        
        public static List<List<T>> Grouped<T>(this IEnumerable<T> messages, int size)
        {
            var groups = new List<List<T>>();
            var list = new List<T>();
            var index = 0;
            foreach (var message in messages)
            {
                list.Add(message);
                if(index != 0 && index % size == 0)
                {
                    groups.Add(list);
                    list = new List<T>();
                }

                index++;
            }
            if(list.Count > 0)
                groups.Add(list);
            return groups;
        }

        public static void AssertAllStagesStopped(this Akka.TestKit.Xunit2.TestKit spec, Action block, IMaterializer materializer)
        {
            AssertAllStagesStopped(spec, () =>
            {
                block();
                return NotUsed.Instance;
            }, materializer);
        }
        
        public static T AssertAllStagesStopped<T>(this Akka.TestKit.Xunit2.TestKit spec, Func<T> block, IMaterializer materializer)
        {
            if (!(materializer is ActorMaterializerImpl impl))
                return block();

            var probe = spec.CreateTestProbe(impl.System);
            probe.Send(impl.Supervisor, StreamSupervisor.StopChildren.Instance);
            probe.ExpectMsg<StreamSupervisor.StoppedChildren>();
            var result = block();

            probe.Within(TimeSpan.FromSeconds(5), () =>
            {
                IImmutableSet<IActorRef> children = ImmutableHashSet<IActorRef>.Empty;
                try
                {
                    probe.AwaitAssert(() =>
                    {
                        impl.Supervisor.Tell(StreamSupervisor.GetChildren.Instance, probe.Ref);
                        children = probe.ExpectMsg<StreamSupervisor.Children>().Refs;
                        if (children.Count != 0)
                            throw new Exception($"expected no StreamSupervisor children, but got {children.Aggregate("", (s, @ref) => s + @ref + ", ")}");
                    });
                }
                catch 
                {
                    children.ForEach(c=>c.Tell(StreamSupervisor.PrintDebugDump.Instance));
                    throw;
                }
            });

            return result;
        }
    }
}