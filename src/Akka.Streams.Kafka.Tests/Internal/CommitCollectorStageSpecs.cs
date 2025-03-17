using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;
using Akka.Streams.Kafka.Tests.TestKit;
using Akka.Streams.Kafka.Tests.TestKit.Internal;
using Akka.Streams.TestKit;
using Akka.TestKit.Extensions;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Internal;

public class CommitCollectorStageSpecs : Akka.TestKit.Xunit2.TestKit
{
    private static readonly Akka.Configuration.Config Config = "akka.loglevel=DEBUG";
    
    public CommitCollectorStageSpecs(ITestOutputHelper output) : base(Config.WithFallback(KafkaExtensions.DefaultSettings), output: output)
    {
        DefaultCommitterSettings = CommitterSettings.Create(Sys);
    }

    public CommitterSettings DefaultCommitterSettings { get; }
    public TimeSpan MessageAbsenceTimeout => TimeSpan.FromSeconds(2);

    [Fact]
    public async Task CommitCollectorStage_when_BatchIsFull_batch_commit_without_errors()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(2).WithMaxInterval(TimeSpan.FromHours(10));
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
        var (msg1, msg2) = (offsetFactory.MakeOffset(), offsetFactory.MakeOffset());
        
        await sinkProbe.RequestAsync(100);
        
        // first message should not be committed but 'batched-up'
        sourceProbe.SendNext(msg1);
        await sourceProbe.ExpectNoMsgAsync(MessageAbsenceTimeout);
        offsetFactory.Committer.Commits.Should().BeEmpty();
        
        // now send second message to complete the batch
        sourceProbe.SendNext(msg2);
        
        var committedBatch = await sinkProbe.ExpectNextAsync();
        
        committedBatch.BatchSize.Should().Be(2);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg2.Offset.Offset);
        offsetFactory.Committer.Commits.Count.Should().Be(1, "expected only one batch commit");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }
    
    [Fact]
    public async Task CommitCollectorStage_when_BatchDurationHasElapsed_batch_commit_without_errors()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(int.MaxValue).WithMaxInterval(TimeSpan.FromMilliseconds(1));
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
      
        await sinkProbe.RequestAsync(100);
        
        var msg = offsetFactory.MakeOffset();
        
        sourceProbe.SendNext(msg);
        var committedBatch = await sinkProbe.ExpectNextAsync();
        committedBatch.BatchSize.Should().Be(1);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg.Offset.Offset);
        offsetFactory.Committer.Commits.Count.Should().Be(1, "expected only one batch commit");
        
        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }
    
    [Fact]
    public async Task CommitCollectorStage_when_BatchDurationHasElapsed_emit_immediately_if_pending_demand()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(int.MaxValue).WithMaxInterval(TimeSpan.FromMilliseconds(1));
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
      
        await sinkProbe.RequestAsync(1);
        
        // interval triggers, but there is no demand
        await sinkProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(200));
        
        // next trigger should emit this single value immediately
        var msg = offsetFactory.MakeOffset();
        
        sourceProbe.SendNext(msg);
        var committedBatch = await sinkProbe.ExpectNextAsync(TimeSpan.FromMilliseconds(50));
        
        committedBatch.BatchSize.Should().Be(1);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg.Offset.Offset);
        offsetFactory.Committer.Commits.Count.Should().Be(1, "expected only one batch commit");
        
        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    private (TestPublisher.Probe<ICommittable> publisher, IControl control,
        TestSubscriber.Probe<ICommittableOffsetBatch> subscriber) StreamProbes(CommitterSettings committerSettings)
    {
        var flow = Committer.BatchFlow(committerSettings);

        var ((source, control), sink) = this.SourceProbe<ICommittable>()
            .ViaMaterialized(ConsumerControlFactory.ControlFlow<ICommittable>(), Keep.Both)
            .Via(flow)
            .ToMaterialized(this.SinkProbe<ICommittableOffsetBatch>(), Keep.Both)
            .Run(Sys);

        return (source, control, sink);
    }

    private (TestPublisher.Probe<ICommittable> publisher, IControl control,
        TestSubscriber.Probe<ICommittableOffsetBatch> subscriber, TestOffsetFactory factory)
        StreamProbesWithOffsetFactory(CommitterSettings committerSettings)
    {
        var (source, control, sink) = StreamProbes(committerSettings);
        var factory = new TestOffsetFactory(new TestBatchCommitter(Sys, committerSettings));
        return (source, control, sink, factory);
    }
}

public static class TestCommittableOffset
{
    public static ICommittableOffset Create(AtomicCounterLong offsetCounter,
        TestBatchCommitter committer, Option<Exception> failWith = default, int partitionNum = 1)
    {
        return new CommittableOffset(committer.Underlying,
            ConsumerResultFactory.PartitionOffset("group1", "topic1", partitionNum,
                offsetCounter.IncrementAndGet()), "metadata1");
    }
}

public class TestOffsetFactory(TestBatchCommitter committer)
{
    public TestBatchCommitter Committer => committer;
    private readonly AtomicCounterLong _offsetCounter = new AtomicCounterLong(0);

    public ICommittableOffset MakeOffset(Option<Exception> failWith = default, int partitionNum = 1)
    {
        return TestCommittableOffset.Create(_offsetCounter, committer, failWith, partitionNum);
    }

    public ICommittableOffsetBatch MakeBatch(Option<Exception> failWith = default, int partitionNum = 1)
    {
        return CommittableOffsetBatch.Create(MakeOffset(failWith, partitionNum));
    }
}

public class TestBatchCommitter
{
    private readonly ActorSystem _system;
    public CommitterSettings CommitSettings { get; }
    public Func<TimeSpan> CommitDelay { get; }
    
    public TestBatchCommitter(ActorSystem system, CommitterSettings commitSettings) : this(system, commitSettings, () => TimeSpan.Zero) { }

    public TestBatchCommitter(ActorSystem system, CommitterSettings commitSettings, Func<TimeSpan> commitDelay)
    {
        _system = system;
        CommitSettings = commitSettings;
        CommitDelay = commitDelay;
        Underlying = new TestCommitterConsumer(this);
    }

    public IImmutableList<TopicPartitionOffset> Commits { get; private set; } =
        ImmutableList<TopicPartitionOffset>.Empty;

    private Task CompleteCommit()
    {
        var promisedCommit = new TaskCompletionSource();
        _system.Scheduler.Advanced.ScheduleOnce(CommitDelay(), () => { promisedCommit.SetResult(); });
        return promisedCommit.Task;
    }

    internal KafkaAsyncConsumerCommitter Underlying { get; }

    private class TestCommitterConsumer : KafkaAsyncConsumerCommitter
    {
        private readonly TestBatchCommitter _committer;

        public TestCommitterConsumer(TestBatchCommitter committer) : base(() => ActorRefs.Nobody,
            committer.CommitSettings.MaxInterval)
        {
            _committer = committer;
        }

        public override Task CommitSingle(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata)
        {
            var commit = new TopicPartitionOffset(topicPartition, offsetAndMetadata.Offset);
            _committer.Commits = _committer.Commits.Add(commit);
            return _committer.CompleteCommit();
        }

        public override Task CommitOneOfMany(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata)
        {
            // CommittableOffsetBatchImpl.offsetsAndMetadata points the next committed message.
            // So to get committed message offset we need to subtract 1
            var commitOffset = offsetAndMetadata.Offset - 1;
            var commit = new TopicPartitionOffset(topicPartition, commitOffset);
            _committer.Commits = _committer.Commits.Add(commit);
            return _committer.CompleteCommit();
        }

        public override void TellCommit(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata,
            bool emergency)
        {
            _ = CommitOneOfMany(topicPartition, offsetAndMetadata);
        }
    }
}