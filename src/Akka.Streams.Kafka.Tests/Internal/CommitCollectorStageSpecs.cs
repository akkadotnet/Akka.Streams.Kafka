using System;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;
using Akka.Streams.Kafka.Tests.TestKit.Internal;
using Akka.Streams.TestKit;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Internal;

public class CommitCollectorStageSpecs : Akka.TestKit.Xunit2.TestKit
{
    public CommitCollectorStageSpecs(ITestOutputHelper output) : base(output: output)
    {
        DefaultCommitterSettings = CommitterSettings.Create(Sys);
    }

    public CommitterSettings DefaultCommitterSettings { get; }
    public TimeSpan MessageAbsenceTimeout => TimeSpan.FromSeconds(2);

    [Fact]
    public async Task CommitCollectorStage_when_BatchIsFull_batch_commit_without_errors()
    {
    }

    //private (TestPublisher.Probe<ICommittable> publisher, IControl control, TestSubscriber.Probe<ICommittableOffsetBatch> subscriber)
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
            
            public override void TellCommit(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata, bool emergency)
            {
                _ = CommitOneOfMany(topicPartition, offsetAndMetadata);
            }
        }
    }