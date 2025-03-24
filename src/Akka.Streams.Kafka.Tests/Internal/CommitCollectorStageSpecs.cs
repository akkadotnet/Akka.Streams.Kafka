// -----------------------------------------------------------------------
//  <copyright file="CommitCollectorStageSpecs.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Pattern;
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
using Debug = System.Diagnostics.Debug;

namespace Akka.Streams.Kafka.Tests.Internal;

public class CommitCollectorStageSpecs : Akka.TestKit.Xunit2.TestKit
{
    private static readonly Akka.Configuration.Config Config = "akka.loglevel=DEBUG";

    public CommitCollectorStageSpecs(ITestOutputHelper output) : base(
        Config.WithFallback(KafkaExtensions.DefaultSettings), output: output)
    {
        DefaultCommitterSettings = CommitterSettings.Create(Sys);
    }

    public CommitterSettings DefaultCommitterSettings { get; }

    public static TimeSpan MessageAbsenceTimeout
    {
        get { return TimeSpan.FromSeconds(2); }
    }

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
        var settings = DefaultCommitterSettings.WithMaxBatch(int.MaxValue)
            .WithMaxInterval(TimeSpan.FromMilliseconds(1));
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
        var settings = DefaultCommitterSettings.WithMaxBatch(int.MaxValue)
            .WithMaxInterval(TimeSpan.FromMilliseconds(1));
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

    [Fact]
    public async Task
        CommitCollectorStage_when_BatchDurationHasElapsed_emit_after_triggered_batch_when_next_Batch_is_full()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(2).WithMaxInterval(TimeSpan.FromMilliseconds(50));
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);

        var msg1 = offsetFactory.MakeOffset();
        await sourceProbe.SendNextAsync(msg1);

        var committedBatch = await sinkProbe.RequestNextAsync();

        var msg2 = offsetFactory.MakeOffset();
        await sourceProbe.SendNextAsync(msg2);
        var msg3 = offsetFactory.MakeOffset();
        await sourceProbe.SendNextAsync(msg3);

        // triggered by size
        var committedBatch2 = await sinkProbe.RequestNextAsync();

        committedBatch.BatchSize.Should().Be(1);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg1.Offset.Offset);

        committedBatch2.BatchSize.Should().Be(2);
        committedBatch2.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch2.Offsets.Last().Offset.Should().Be(msg3.Offset.Offset);

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStages should batch commit all elements if upstream has suddenly completed")]
    public async Task
        CommitCollectorStageWhenOffetsAreInFlightBatchCommitAllBufferedElementsIfUpstreamHasSuddenlyCompleted()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(2).WithMaxInterval(TimeSpan.FromHours(10))
            .WithParallelism(1);
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);

        await sinkProbe.EnsureSubscriptionAsync();
        await sinkProbe.RequestAsync(100);

        var msg1 = offsetFactory.MakeOffset();
        await sourceProbe.SendNextAsync(msg1);
        await sourceProbe.SendCompleteAsync();

        var committedBatch = await sinkProbe.ExpectNextAsync();

        committedBatch.BatchSize.Should().Be(1);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg1.Offset.Offset);
        offsetFactory.Committer.Commits.Count.Should().Be(1, "expected only one batch commit");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStages should batch commit all elements if upstream has suddenly completed with delayed commits")]
    public async Task
        CommitCollectorStageWhenOffetsAreInFlightBatchCommitAllElementsIfUpstreamHasCompletedSuddenlyWithDelayedCommits()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(2).WithMaxInterval(TimeSpan.FromHours(10))
            .WithParallelism(1);
        var (sourceProbe, control, sinkProbe) = StreamProbes(settings);
        var committer = new TestBatchCommitter(Sys, settings, () => TimeSpan.FromMilliseconds(50));
        var offsetFactory = new TestOffsetFactory(committer);

        await sinkProbe.RequestAsync(100);
        var (msg1, msg2) = (offsetFactory.MakeOffset(), offsetFactory.MakeOffset());

        await sourceProbe.SendNextAsync(msg1);
        await sourceProbe.SendNextAsync(msg2);
        await sourceProbe.SendCompleteAsync();

        var committedBatch = await sinkProbe.ExpectNextAsync();

        committedBatch.BatchSize.Should().Be(2);
        committedBatch.Offsets.Count.Should().Be(1); // 1 offset value per partition
        committedBatch.Offsets.Last().Offset.Should().Be(msg2.Offset.Offset);
        offsetFactory.Committer.Commits.Count.Should().Be(1, "expected only one batch commit");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStages should batch commit all elements if upstream has suddenly failed")]
    public async Task
        CommitCollectorStageWhenOffetsAreInFlightBatchCommitAllElementsIfUpstreamHasSuddenlyFailed()
    {
        // special config to have more than one batch failure 
        var settings = DefaultCommitterSettings.WithMaxBatch(3).WithMaxInterval(TimeSpan.FromHours(10))
            .WithParallelism(100);

        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);

        await sinkProbe.RequestAsync(100);

        var msgs = Enumerable.Range(1, 10).Select(_ => offsetFactory.MakeOffset()).ToList();

        foreach (var msg in msgs)
        {
            await sourceProbe.SendNextAsync(msg);
        }

        var testException = new IllegalStateException("BOOM!");
        await sourceProbe.SendErrorAsync(testException);

        var receivedError = await PullTillFailureAsync(sinkProbe, 4);
        receivedError.Should().Be(testException);

        var commits = offsetFactory.Committer.Commits;
        commits[^1].Offset.Value.Should().Be(10, "last offset commit should be exactly the one preceeding the failure");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStage using NextObservedOffset should only commit when next offset is observed")]
    public async Task CommitCollectorUsingNextObservedOffsetShouldOnlyCommitWhenNextOffsetIsObserved()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(1).WithCommitWhen(CommitWhen.NextOffsetObserved.Instance);
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
        var (msg1, msg2, msg3) = (offsetFactory.MakeOffset(), offsetFactory.MakeOffset(), offsetFactory.MakeOffset());

        await sinkProbe.RequestAsync(100);

        // first message should not be committed but be 'batched-up' again
        await sourceProbe.SendNextAsync(msg1);
        await sourceProbe.SendNextAsync(msg2);
        await sourceProbe.SendNextAsync(msg3);

        var batches = await sinkProbe.ExpectNextNAsync(2).ToListAsync();
        await sinkProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(10));

        // batches are committed using SelectAsyncUnordered, so order is not guaranteed
        var lastBatch = batches.MaxBy(c => c.Offsets.Last().Offset.Value);

        Assert.NotNull(lastBatch);
        lastBatch.Offsets.Last().Offset.Should()
            .Be(msg2.Offset.Offset, "expected only second message to be committed");
        offsetFactory.Committer.Commits.Count.Should().Be(2, "expected only two commits");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStage using NextObservedOffset should only commit when next offset is observed in a CommittableOffsetBatch")]
    public async Task CommitCollectorUsingNextObservedOffsetShouldOnlyCommitWhenNextOffsetIsObservedInBatch()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(1).WithCommitWhen(CommitWhen.NextOffsetObserved.Instance);
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
        // create batches of size 1
        var (batch1, batch2, batch3) =
            (offsetFactory.MakeBatch(), offsetFactory.MakeBatch(), offsetFactory.MakeBatch());

        await sinkProbe.RequestAsync(100);

        // first message should not be committed but be 'batched-up' again
        await sourceProbe.SendNextAsync(batch1);
        await sourceProbe.SendNextAsync(batch2);
        await sourceProbe.SendNextAsync(batch3);

        var batches = await sinkProbe.ExpectNextNAsync(2).ToListAsync();
        await sinkProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(10));

        // batches are committed using SelectAsyncUnordered, so order is not guaranteed
        var lastBatch = batches.MaxBy(c => c.Offsets.Last().Offset.Value);

        Assert.NotNull(lastBatch);
        lastBatch.Offsets.Last().Offset.Should()
            .Be(batch2.Offsets.First().Offset, "expected only second message to be committed");
        offsetFactory.Committer.Commits.Count.Should().Be(2, "expected only two commits");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStage using NextObservedOffset should only commit when next offset is observed in a CommittableOffset preceded by a CommittableOffsetBatch")]
    public async Task
        CommitCollectorUsingNextObservedOffsetShouldOnlyCommitWhenNextOffsetIsOffsetPrecededObservedInBatch()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(1).WithCommitWhen(CommitWhen.NextOffsetObserved.Instance);
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
        // create batches of size 1
        var (batch1, msg2, batch3) = (offsetFactory.MakeBatch(), offsetFactory.MakeOffset(), offsetFactory.MakeBatch());

        await sinkProbe.RequestAsync(100);

        // first message should not be committed but be 'batched-up' again
        await sourceProbe.SendNextAsync(batch1);
        await sourceProbe.SendNextAsync(msg2);
        await sourceProbe.SendNextAsync(batch3);

        var batches = await sinkProbe.ExpectNextNAsync(2).ToListAsync();
        await sinkProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(10));

        // batches are committed using SelectAsyncUnordered, so order is not guaranteed
        var lastBatch = batches.MaxBy(c => c.Offsets.Last().Offset.Value);

        Assert.NotNull(lastBatch);
        lastBatch.Offsets.Last().Offset.Should()
            .Be(msg2.Offset.Offset, "expected only second message to be committed");
        offsetFactory.Committer.Commits.Count.Should().Be(2, "expected only two commits");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    [Fact(DisplayName =
        "CommitCollectorStage using NextObservedOffset should only commit when next offset is observed for correct partitions")]
    public async Task
        CommitCollectorUsingNextObservedOffsetShouldOnlyCommitWhenNextOffsetIsObservedForCorrectPartitions()
    {
        var settings = DefaultCommitterSettings.WithMaxBatch(1).WithCommitWhen(CommitWhen.NextOffsetObserved.Instance);
        var (sourceProbe, control, sinkProbe, offsetFactory) = StreamProbesWithOffsetFactory(settings);
        // create batches of size 1
        var (msg1, msg2, msg3, msg4, msg5)
            = (offsetFactory.MakeOffset(partitionNum: 1), offsetFactory.MakeOffset(partitionNum: 2),
                offsetFactory.MakeOffset(partitionNum: 1), offsetFactory.MakeOffset(partitionNum: 2),
                offsetFactory.MakeOffset(partitionNum: 1));

        var allMessages = new[] { msg1, msg2, msg3, msg4, msg5 };

        await sinkProbe.RequestAsync(100);

        await Task.WhenAll(allMessages.Select(c => sourceProbe.SendNextAsync(c)));

        var batches = await sinkProbe.ExpectNextNAsync(3).ToListAsync();
        await sinkProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(10));

        // batches are committed using SelectAsyncUnordered, so order is not guaranteed
        // Get the last 2 batches
        var lastBatches = batches.OrderByDescending(c => c.Offsets.Last().Offset.Value)
            .Take(2).ToList();
        var lastBatch = lastBatches[0];
        var secondLastBatch = lastBatches[1];

        lastBatch.Offsets.Should().Contain(msg3.Offset, "expected the second offset of partition 1");
        secondLastBatch.Offsets.Should().Contain(msg2.Offset, "expected the first offset of partition 2");
        offsetFactory.Committer.Commits.Count.Should().Be(3, "expected only three commits");

        await control.Shutdown().WaitAsync(RemainingOrDefault);
    }

    private async Task<Exception?> PullTillFailureAsync(TestSubscriber.Probe<ICommittableOffsetBatch> sinkProbe,
        int maxEvents)
    {
        while (true)
        {
            var nextError = sinkProbe.ExpectNextOrErrorAsync();
            if (maxEvents < 0) Assert.Fail("Max number of events have been read without failure");

            var m = await nextError;
            switch (m)
            {
                case Exception ex:
                    Log.Debug("Received error: {0}", nextError);
                    return ex;
                default:
                    Log.Debug("Received batch: {0}", m);
                    maxEvents -= 1;
                    continue;
            }
        }
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
        TestBatchCommitter committer, Option<Exception> failWith = default, int partitionNum = 1) =>
        new CommittableOffset(committer.Underlying,
            ConsumerResultFactory.PartitionOffset("group1", "topic1", partitionNum,
                offsetCounter.IncrementAndGet()), "metadata1");
}

public class TestOffsetFactory(TestBatchCommitter committer)
{
    public TestBatchCommitter Committer
    {
        get { return committer; }
    }

    private readonly AtomicCounterLong _offsetCounter = new(0);

    public ICommittableOffset MakeOffset(Option<Exception> failWith = default, int partitionNum = 1) =>
        TestCommittableOffset.Create(_offsetCounter, committer, failWith, partitionNum);

    public ICommittableOffsetBatch MakeBatch(Option<Exception> failWith = default, int partitionNum = 1) =>
        CommittableOffsetBatch.Create(MakeOffset(failWith, partitionNum));
}

public class TestBatchCommitter
{
    private readonly ActorSystem _system;
    public CommitterSettings CommitSettings { get; }
    public Func<TimeSpan> CommitDelay { get; }

    public TestBatchCommitter(ActorSystem system, CommitterSettings commitSettings) : this(system, commitSettings,
        () => TimeSpan.Zero)
    {
    }

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
            // CommittableOffsetBatch.OffsetsAndMetadata points the next committed message.
            // So to get committed message offset we need to subtract 1
            var commitOffset = offsetAndMetadata.Offset - 1;
            var commit = new TopicPartitionOffset(topicPartition, commitOffset);
            _committer.Commits = _committer.Commits.Add(commit);
            return _committer.CompleteCommit();
        }

        public override void TellCommit(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata,
            bool emergency) =>
            _ = CommitOneOfMany(topicPartition, offsetAndMetadata);
    }
}