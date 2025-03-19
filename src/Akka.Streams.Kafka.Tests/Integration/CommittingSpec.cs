using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class CommittingSpec : KafkaIntegrationTests
{
    
    public CommittingSpec(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(CommittingSpec), output, fixture)
    {
    }

    private static readonly string[] Numbers = Enumerable.Range(1, 200).Select(c => c.ToString()).ToArray();
    private const int Partition1 = 1;

    [Fact]
    public async Task CommittingMustEnsureUncommittedMessagesAreRedelivered()
    {
        var messages = Numbers.Take(100).ToArray();
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var group2 = CreateGroup(2);

        await ProduceStrings(new TopicPartition(topic1, new Partition(0)), messages, ProducerSettings);

        var committedElements = new AtomicCounter(0);
        var consumerSettings = CreateConsumerSettings<string>(group1)
            .WithVerboseLogging(true);

        var (control, probe1) = KafkaConsumer
            .CommittableSource(consumerSettings, Subscriptions.Topics(topic1))
            .SelectAsync(10, async e =>
            {
                await ((CommittableOffset)e.CommitableOffset).Commit();
                var offsetAsInt = (int)e.CommitableOffset.Offset.Offset.Value;
                var curValue = committedElements.Current;

                // CAS to ensure that we only commit the highest offset
                // N.B. if this racy, replace with messaging an actor
                committedElements.CompareAndSet(curValue, Math.Max(curValue, offsetAsInt));

                Sys.Log.Info("Committed: {0}", offsetAsInt);
                return e.Record.Message.Value;
            })
            .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
            .Run(Sys);

        await probe1.RequestAsync(25);
        var found = (await probe1.ExpectNextNAsync(25).ToListAsync());
        messages.Take(25).Should().BeEquivalentTo(found);

        await probe1.CancelAsync();
        await control.IsShutdown;

        var probe2 = KafkaConsumer
            .CommittableSource(consumerSettings, Subscriptions.Topics(topic1))
            .Select(c => c.Record.Message.Value)
            .RunWith(this.SinkProbe<string>(), Sys);

        // Note that due to buffers and SelectAsync(10) the committed offset is more
        // than 26, and that is not wrong

        // some concurrent publishing
        await ProduceStrings(new TopicPartition(topic1, new Partition(0)), messages.Skip(100), ProducerSettings);

        var expectedResumed = messages.Skip(committedElements.Current).ToList();
        await probe2.RequestAsync(Numbers.Length);
        await probe2.ExpectNextNAsync(expectedResumed);

        await probe2.CancelAsync();

        // another consumer from a different group should get all the messages
        var probe3 = KafkaConsumer
            .CommittableSource(consumerSettings.WithGroupId(group2), Subscriptions.Topics(topic1))
            .Select(c => c.Record.Message.Value)
            .RunWith(this.SinkProbe<string>(), Sys);

        await probe3.AsyncBuilder()
            .Request(messages.Length)
            .ExpectNextN(messages)
            .ExecuteAsync();

        await probe3.CancelAsync();
    }

    /*
     * Kind of a stupid spec - just because we haven't gotten the new partition assignment yet
     * doesn't mean that the rebalance is-in progress. It's just that we haven't gotten the new assignment
     * message from the second half of the rebalancing operation yet.
     */
    [Fact]
    public async Task CommittingMustWorkEvenIfThePartitionIsBalancedAwayAndNotReassignedYet()
    {
        var count = 10;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var consumerSettings = CreateConsumerSettings<string>(group1);

        var partition0 = new TopicPartition(topic1, new Partition(0));
        var partition1 = new TopicPartition(topic1, new Partition(1));

        await GivenInitializedTopicAsync(topic1, 2);

        await Source.From(Numbers.Take(10))
            .Select(n =>
            {
                return ProducerMessage.Multi([
                    new ProducerRecord<Null, string>(partition0, n + "-p0"),
                    new ProducerRecord<Null, string>(partition1, n + "-p1")
                ]);
            })
            .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(ProducerSettings))
            .RunWith(Sink.Ignore<IResults<Null, string, NotUsed>>(), Sys);

        // Subscribe to the topic (without demand)
        var rebalanceActor1 = CreateTestProbe();
        var subscription1 = Subscriptions.Topics(topic1).WithRebalanceListener(rebalanceActor1.Ref);
        var (control1, probe1) = KafkaConsumer.CommittableSource(consumerSettings, subscription1)
            .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
            .Run(Sys);

        // Await initial partition assignment
        var tp1 = await rebalanceActor1.ExpectMsgAsync<TopicPartitionsAssigned>();
        tp1.Partitions.Should().BeEquivalentTo([partition0, partition1]);
        tp1.Subscription.Should().Be(subscription1);

        // read all messages from both partitions
        var committables1 = await probe1.AsyncBuilder()
            .Request(count * 2).ExpectNextNAsync(count * 2).ToListAsync();

        // Subscribe to topic (without demand)
        var rebalanceActor2 = CreateTestProbe();
        var subscription2 = Subscriptions.Topics(topic1).WithRebalanceListener(rebalanceActor2.Ref);
        var (control2, probe2) = KafkaConsumer.CommittableSource(consumerSettings, subscription2)
            .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
            .Run(Sys);

        // Await an assignment to the new rebalance listener
        var tp2 = await rebalanceActor2.ExpectMsgAsync<TopicPartitionsAssigned>();

        // Await revoke of all partitions in old rebalance listener
        await rebalanceActor1.ExpectMsgAsync<TopicPartitionsRevoked>();

        // commit BEFORE the reassign finishes with an assignment
        var consumer1Read = Task.WhenAll(committables1.Select(async c =>
        {
            await ((CommittableOffset)c.CommitableOffset).Commit();
            return c.Record.Message.Value;
        }));

        // the rebalance finishes
        await rebalanceActor1.ExpectMsgAsync<TopicPartitionsAssigned>();

        var finalResults = await consumer1Read;
        finalResults.Should().BeEquivalentTo(Numbers.Take(count).Select(c => c + "-p0")
            .Concat(Numbers.Take(count).Select(c => c + "-p1")));
        
        probe1.Cancel();
        probe2.Cancel();
        
        await control1.IsShutdown;
        await control2.IsShutdown;
    }

    [Fact]
    public async Task CommittingMustIgnoreCommitsToPartitionsThatGotRevoked()
    {
        var count = 10;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var consumerSettings = CreateConsumerSettings<string>(group1);

        var partition0 = new TopicPartition(topic1, new Partition(0));
        var partition1 = new TopicPartition(topic1, new Partition(1));

        await GivenInitializedTopicAsync(topic1, 2);
        
        await Source.From(Numbers.Take(10))
            .Select(n =>
            {
                return ProducerMessage.Multi([
                    new ProducerRecord<Null, string>(partition0, n + "-p0"),
                    new ProducerRecord<Null, string>(partition1, n + "-p1")
                ]);
            })
            .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(ProducerSettings))
            .RunWith(Sink.Ignore<IResults<Null, string, NotUsed>>(), Sys);
        
        // Subscribe to the topic (without demand)
        var rebalanceActor1 = CreateTestProbe();
        var subscription1 = Subscriptions.Topics(topic1).WithRebalanceListener(rebalanceActor1.Ref);
        var (control1, probe1) = KafkaConsumer.CommittableSource(consumerSettings, subscription1)
            .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
            .Run(Sys);

        // Await initial partition assignment
        var tp1 = await rebalanceActor1.ExpectMsgAsync<TopicPartitionsAssigned>();
        tp1.Partitions.Should().BeEquivalentTo([partition0, partition1]);
        tp1.Subscription.Should().Be(subscription1);

        // read all messages from both partitions
        var committables1 = await probe1.AsyncBuilder()
            .Request(count * 2).ExpectNextNAsync(count * 2).ToListAsync();

        // Subscribe to topic (without demand)
        var rebalanceActor2 = CreateTestProbe();
        var subscription2 = Subscriptions.Topics(topic1).WithRebalanceListener(rebalanceActor2.Ref);
        var (control2, probe2) = KafkaConsumer.CommittableSource(consumerSettings, subscription2)
            .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
            .Run(Sys);
        
        // Rebalance fully completes
        var tp2 = await rebalanceActor2.ExpectMsgAsync<TopicPartitionsAssigned>();
        await rebalanceActor1.ExpectMsgAsync<TopicPartitionsRevoked>();
        await rebalanceActor1.ExpectMsgAsync<TopicPartitionsAssigned>();

        var partitionRevokedFromConsumer1 = tp2.Partitions.Single();

        // commit all messages from consumer 1
        var consumer1Read = Task.WhenAll(committables1.Select(async c =>
        {
            await ((CommittableOffset)c.CommitableOffset).Commit();
            return c.Record.Message.Value;
        }));
        
        var committables2 = await probe2.AsyncBuilder()
            .Request(count).ExpectNextNAsync(count ).ToListAsync();
        
        // messages that belonged to the revoked partition show up in the new consumer,
        // even though they were committed after the rebalance
        var recordSuffix = partitionRevokedFromConsumer1.Partition.Value switch
        {
            0 => "p0",
            1 => "p1",
            _ => throw new ArgumentException("Unexpected partition")
        };

        var consumer2Read = committables2.Select(c => c.Record.Message.Value);
        var expectedResults = Numbers.Take(count).Select(c => c + "-" + recordSuffix);
        
        consumer2Read.Should().BeEquivalentTo(expectedResults);

        await probe1.CancelAsync();
        await probe2.CancelAsync();
        
        await control1.IsShutdown;
        await control2.IsShutdown;
    }
}