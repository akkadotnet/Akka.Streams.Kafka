using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class RebalanceExtTests : KafkaIntegrationTests
{
    public RebalanceExtTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(RebalanceExtTests), output, fixture)
    {
    }

    private const string ConsumerClientId1 = "consumer-1";
    private const string ConsumerClientId2 = "consumer-2";

    private sealed record TopicPartitionMetadata(
        IReadOnlyCollection<string> Topics,
        IReadOnlyCollection<TopicPartition> Partitions,
        IReadOnlyDictionary<string, TaskCompletionSource<Done>> TpFutureMap,
        IReadOnlyCollection<Task<Done>> ProducerTpsAck,
        IReadOnlyDictionary<int, MessageAck> MessageAndStoreAck);

    private sealed record MessageAck(
        string PartitionName,
        AtomicCounter MessageCounter,
        TaskCompletionSource<Done> WaitUntil,
        TaskCompletionSource<Done> AckWaitUntil);

    private sealed class LoggingAssignmentHandler(string clientId, ILoggingAdapter log) : IPartitionEventHandler
    {
        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
            IRestrictedConsumer consumer) =>
            log.Debug("AssignmentHandler::OnRevoke: clientId {0} tps {1} consumer {2}", clientId,
                revokedTopicPartitions, consumer);

        public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer) =>
            log.Debug("AssignmentHandler::OnLost: clientId {0} tps {1} consumer {2}", clientId,
                revokedTopicPartitions, consumer);

        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer) =>
            log.Debug("AssignmentHandler::OnAssign: clientId {0} tps {1} consumer {2}", clientId,
                assignedTopicPartitions, consumer);

        public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer) =>
            log.Debug("AssignmentHandler::OnStop: clientId {0} tps {1} consumer {2}", clientId,
                topicPartitions, consumer);
    }

    private ConsumerSettings<Null, string> CreateConsumerSettings(string groupId, int maxPollRecords) =>
        CreateConsumerSettings<Null, string>(groupId)
            .WithPollInterval(TimeSpan.FromMilliseconds(400))
            .WithMaxPollRecords(maxPollRecords);

    private (IControl control, Task<IImmutableList<Task<Done>>> tasks) SubscribeAndConsumeMessages(string clientId,
        int perPartitionMessageCount, TopicPartitionMetadata topicMetaData,
        ConsumerSettings<Null, string> consumerSettings,
        IAutoSubscription subscription, SharedKillSwitch sharedKillSwitch)
    {
        var logPrefix = $"[{clientId}]";
        return KafkaConsumer.CommittablePartitionedSource(consumerSettings
                    .WithClientId(clientId),
                subscription.WithPartitionEventsHandler(new LoggingAssignmentHandler(clientId, Log)))
            .Select(tuple =>
            {
                var (tp, source) = tuple;
                Log.Debug("{0} Consuming partitioned source clientId: {1}, for tp: {2}", logPrefix, clientId, tp);
                var innerStream = source
                    .Via(sharedKillSwitch.Flow<CommittableMessage<Null, string>>())
                    .Via(BusinessFlow(clientId, perPartitionMessageCount, topicMetaData, logPrefix))
                    .Via(Committer.BatchFlow(CommitterSettings.WithMaxBatch(1)));

                return innerStream.RunWith(Sink.Ignore<ICommittableOffsetBatch>(), Materializer);
            })
            .Via(sharedKillSwitch.Flow<Task<Done>>())
            .ToMaterialized(Sink.Seq<Task<Done>>(), Keep.Both)
            .Run(Materializer);
    }

    private Flow<CommittableMessage<Null, string>, ICommittable, NotUsed> BusinessFlow(string clientId,
        int perPartitionMessageCount, TopicPartitionMetadata topicMetadata, string logPrefix)
    {
        return Flow.FromFunction<CommittableMessage<Null, string>, Task<ICommittable>>(ProcessMsg)
            .SelectAsync(1, async t => await t);

        async Task<ICommittable> ProcessMsg(CommittableMessage<Null, string> message)
        {
            var messageVal = int.Parse(message.Record.Message.Value);
            var duplicateCount = topicMetadata.MessageAndStoreAck[messageVal].MessageCounter.IncrementAndGet();
            var partitionName = $"{message.Record.Topic}-{message.Record.Partition.Value}";
            var msg1 =
                $"{logPrefix}::businessFlow:offset={message.CommitableOffset.Offset.Offset} messageId={messageVal} partition={partitionName} consumerId={clientId} duplicateCount={duplicateCount}";

            if (duplicateCount > 1)
            {
                Log.Warning("businessFlow:duplicate:{0}", msg1);
            }
            else
            {
                Log.Info("businessFlow:received:{0}", msg1);
            }

            // Flow of execution is blocked, but not the thread
            Log.Info("{0}::businessFlow::begin:blockAtMessage={1}", logPrefix, msg1);
            await topicMetadata.MessageAndStoreAck[messageVal].WaitUntil.Task.WaitAsync(RemainingOrDefault);
            var ackWaitUntilPromise = topicMetadata.MessageAndStoreAck[messageVal].AckWaitUntil;
            if (ackWaitUntilPromise.Task.IsCompleted)
            {
                Log.Info("{0}::businessFlow::ignore:blockAtMessage={1}", logPrefix, msg1);
            }
            else
            {
                ackWaitUntilPromise.TrySetResult(Done.Instance);
                Log.Info("{0}::businessFlow::end:blockAtMessage={1}", logPrefix, msg1);
            }

            if (message.CommitableOffset.Offset.Offset == perPartitionMessageCount - 1)
            {
                var lastMessagePromise = topicMetadata.TpFutureMap[partitionName];
                if (lastMessagePromise.Task.IsCompleted)
                    Log.Warning("{0}::businessFlow::promise:already completed={1}", logPrefix, msg1);
                else
                {
                    Log.Info("{0}::businessFlow::promise:completing={1}", logPrefix, msg1);
                    lastMessagePromise.TrySetResult(Done.Instance);
                }
            }

            return message.CommitableOffset;
        }
    }

    private async Task<TopicPartitionMetadata> CreateTopicMapsAndPublishMessagesAsync(int topicCount,
        int partitionCount,
        int perPartitionMessageCount)
    {
        var tps = new List<TopicPartition>();
        var topics = new List<string>();
        var tpFutureMap = new Dictionary<string, TaskCompletionSource<Done>>();
        foreach (var i in Enumerable.Range(1, topicCount))
        {
            var topic1 = CreateTopic(1);
            await GivenInitializedTopicAsync(topic1, partitionCount);
            Log.Debug("Created topic {0}", topic1);
            topics.Add(topic1);
            foreach (var j in Enumerable.Range(0, partitionCount))
            {
                tps.Add(new TopicPartition(topic1, j));
                tpFutureMap[$"{topic1}-{j}"] = new TaskCompletionSource<Done>();
            }
        }

        var (producerTpsAck, messageAndStoreAck) =
            PublishMessages(partitionCount, perPartitionMessageCount, topics);

        return new TopicPartitionMetadata(topics, tps, tpFutureMap, producerTpsAck, messageAndStoreAck);
    }

    private
        (List<Task<Done>> producerTpsAck, Dictionary<int, MessageAck> messageAndStoreAck)
        PublishMessages(int partitionCount, int perPartitionMessageCount, IReadOnlyList<string> topics)
    {
        var messageAndStoreAck = new Dictionary<int, MessageAck>();
        var producerTpsAck = new List<Task<Done>>();
        var topicIndex = -1;
        foreach (var topic in topics)
        {
            topicIndex += 1;
            var topicOffset = topicIndex * partitionCount * perPartitionMessageCount;
            foreach (var partitionIdx in Enumerable.Range(0, partitionCount))
            {
                var startMessageIndx = partitionIdx * perPartitionMessageCount + 1 + topicOffset;
                var endMessageIndx = startMessageIndx + perPartitionMessageCount - 1;
                var messageRange = Enumerable.Range(startMessageIndx, perPartitionMessageCount).ToList();
                var topicPartition = new TopicPartition(topic, new Partition(partitionIdx));
                var partitionName = $"{topic}-{partitionIdx}";
                foreach (var messageId in messageRange)
                {
                    messageAndStoreAck[messageId] = new MessageAck(partitionName, new AtomicCounter(0),
                        new TaskCompletionSource<Done>(), new TaskCompletionSource<Done>());
                }

                producerTpsAck.Add(DoProduce());
                continue;

                async Task<Done> DoProduce()
                {
                    await ProduceStrings(topicPartition, messageRange, ProducerSettings);
                    Log.Debug("publishMessages:published messages from ({0} to {1}) for partition name {2}",
                        startMessageIndx, endMessageIndx,
                        partitionName);
                    return Done.Instance;
                }
            }
        }

        return (producerTpsAck, messageAndStoreAck);
    }

    [Fact(DisplayName =
        "Fetched records must not be lost when two consumers consume from one topic and two partitions and one consumer aborts mid-stream")]
    public async Task FetchedRecordsMustNotBeLostUponAbort()
    {
        await WithinAsync(TimeSpan.FromSeconds(30), async () =>
        {
            var topicCount = 1;
            var partitionCount = 2;
            var perPartitionMessageCount = 9;

            // create topic-partition map and publish messages
            // messageId(1 to 9) => topic-1-1-0
            // messageId(10 to 18) => topic-1-1-1
            var topicMetadata =
                await CreateTopicMapsAndPublishMessagesAsync(topicCount, partitionCount, perPartitionMessageCount);

            var groupId = CreateGroup(1);
            var consumerSettings1 = CreateConsumerSettings(groupId, 3);

            // Let the producers publish all messages
            await Task.WhenAll(topicMetadata.ProducerTpsAck).WaitAsync(RemainingOrDefault);

            // Can't manually assign partitions in the .NET driver, so we'll have to see what the broker does

            // consumer-1::introduce first consumer with topic-1-1-0 assigned to its SubSource-topic-1-1-0-A
            var probe1RebalanceActor = CreateTestProbe();
            var subscription = Subscriptions.Topics(topicMetadata.Topics.ToArray())
                .WithRebalanceListener(probe1RebalanceActor);
            var sharedKillSwitch1 = KillSwitches.Shared(ConsumerClientId1);

            var (control1, tasks1) = SubscribeAndConsumeMessages(ConsumerClientId1, perPartitionMessageCount,
                topicMetadata,
                consumerSettings1, subscription, sharedKillSwitch1);

            // consumer-1 is going to get assigned both partitions
            await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>();

            // consumer-1::SubSource-topic-1-1-0-A:confirm first messageId=1 is received and committed from message batch (1,2,3)
            topicMetadata.MessageAndStoreAck[1].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[1].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-1::SubSource-topic-1-1-0-A:verify messageId=1 is received in the business logic function
            Assert.Equal(1, topicMetadata.MessageAndStoreAck[1].MessageCounter.Current);

            // consumer-2::introduce second consumer with topic-1-1-1 assigned to its SubSource-1-1-1-A
            var probe2RebalanceActor = CreateTestProbe();
            var subscription2 = subscription.WithRebalanceListener(probe2RebalanceActor);
            var sharedKillSwitch2 = KillSwitches.Shared(ConsumerClientId2);

            var (control2, tasks2) = SubscribeAndConsumeMessages(ConsumerClientId2, perPartitionMessageCount,
                topicMetadata,
                consumerSettings1, subscription2, sharedKillSwitch2);

            // let's see how gets assigned what
            var partitionsRevoked = await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsRevoked>();
            Assert.Equal(partitionCount, partitionsRevoked.Partitions.Count);

            var consumer1Partitions = await probe1RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>();
            var consumer2Partitions = await probe2RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>();
            Log.Info("consumer1Partitions: {0} -> consumer2Partitions:", string.Join(", ", consumer1Partitions),
                string.Join(", ", consumer2Partitions));

            // figure out how the Assignor assigned the partitions
            var doesConsumer1HaveFirstPartition = consumer1Partitions.Partitions.Single().Partition.Value == 0;

            /*
             * So here's what sucks: since we can't control the Assignor, it's totally possible that these are left-overs
             * from Consumer-1. We'll find out if we see the duplicate count go above 1.
             */

            // consumer-1::SubSource-topic-1-1-1-A:confirm first messageId=10 is received and committed from batch (10,11,12)
            topicMetadata.MessageAndStoreAck[10].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[10].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-1::SubSource-topic-1-1-1-A:verify messageId=10 is received in the business logic function
            // Assert.Equal(1, topicMetadata.MessageAndStoreAck[10].MessageCounter.Current); // due to lack of manual partition assignment, no guarantee that this is 1

            // consumer-1::SubSource-topic-1-1-0-A:confirm first messageId=2 is received and committed from batch (1,2,3)
            topicMetadata.MessageAndStoreAck[2].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[2].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // abort Consumer-1
            sharedKillSwitch1.Abort(new Exception($"abort {ConsumerClientId1} messageId=2"));

            // consumer-2::after abort two new sub sources serve topic-1-1-0 and topic-1-1-1: SubSource-topic-1-1-0-B and SubSource-topic-1-1-1-B

            // consumer-2::SubSource-topic-1-1-1-A:unblock second message from batch (10,11,12)
            topicMetadata.MessageAndStoreAck[11].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[11].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-2::SubSource-topic-1-1-0-B starts consuming at its first batch (2,3,4)
            // consumer-2::SubSource-topic-1-1-1-B starts consuming at its first batch (12,13,14)

            // consumer-1::SubSource-topic-1-1-0-A:unblock last message from batch (1,2,3)
            // consumer-2::SubSource-topic-1-1-0-B:unblock second message from batch (2,3,4)
            topicMetadata.MessageAndStoreAck[3].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[3].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-1::SubSource-topic-1-1-0-A:Terminates

            // wait until the rebalance happens
            await probe2RebalanceActor.ExpectMsgAsync<TopicPartitionsRevoked>();
            var assigned2again = await probe2RebalanceActor.ExpectMsgAsync<TopicPartitionsAssigned>();
            Assert.Equal(partitionCount, assigned2again.Partitions.Count);

            // consumer-2::SubSource-topic-1-1-0-B:unblock last message from batch (2,3,4), messages (2,3) are re-played
            topicMetadata.MessageAndStoreAck[4].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[4].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-2::SubSource-topic-1-1-0-B:issues RequestMessage for the next batch

            // consumer-2::SubSource-topic-1-1-0-B:unblock first message from batch (5,6,7)
            topicMetadata.MessageAndStoreAck[5].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[5].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            // consumer-2::SubSource-topic-1-1-1-A:unblock last message from batch (10,11,12)
            // consumer-2::SubSource-topic-1-1-1-A:issues **problematic** RequestMessage requesting the next batch
            // consumer-2::SubSource-topic-1-1-1-B:unblock first message from batch (12,13,14), message 12 is re-played
            topicMetadata.MessageAndStoreAck[12].WaitUntil.TrySetResult(Done.Instance);
            await topicMetadata.MessageAndStoreAck[12].AckWaitUntil.Task.WaitAsync(RemainingOrDefault);

            /* without having manual partition assignments, the rest of RebalanceExtSpec is somewhat
             non-deterministic. Therefore, we're just going to consume all the remaining messages and
             analyze the results afterward - looking for gaps and duplicates.

             The alternative to doing this is re-structuring the test to limit event production to a single partition
             at a time, which is certainly doable, but would require a lot of re-writing.
             */

            // ReSharper disable once UselessBinaryOperation
            var publishedMessageCount = topicCount * partitionCount * perPartitionMessageCount;
            foreach (var messageId in Enumerable.Range(1, publishedMessageCount))
            {
                if (!topicMetadata.MessageAndStoreAck[messageId].WaitUntil.Task.IsCompleted)
                    topicMetadata.MessageAndStoreAck[messageId].WaitUntil.TrySetResult(Done.Instance);
            }

            // wait until the last message from each partition has been consumed
            await Task.WhenAll(topicMetadata.TpFutureMap.Values.Select(t => t.Task)).WaitAsync(RemainingOrDefault);

            // shutdown the consumers
            await Task.WhenAll(control1.Shutdown(), control2.Shutdown()).WaitAsync(RemainingOrDefault);
            sharedKillSwitch1.Shutdown();
            sharedKillSwitch2.Shutdown();

            // analyze received messages
            var consumedMessages = topicMetadata.MessageAndStoreAck.Where(c => c.Value.MessageCounter.Current > 0)
                .ToList();
            Log.Debug("consumedMessages.Count: {0} publishedMessageCount={1}", consumedMessages.Count,
                publishedMessageCount);

            if (consumedMessages.Count != publishedMessageCount)
            {
                var consumedMessageIds = consumedMessages.Select(c => c.Key).ToList();
                var missingMessages = Enumerable.Range(1, publishedMessageCount).Except(consumedMessageIds).ToList();
                Log.Error("missingMessages: {0}", string.Join(", ", missingMessages));
            }

            var duplicateMessages = consumedMessages.Where(c => c.Value.MessageCounter.Current > 1).ToList();
            if (duplicateMessages.Count > 0)
            {
                Log.Error("duplicateMessages: {0}", string.Join(", ", duplicateMessages.Select(c => c.Key)));
            }
            
            // need to assert that we did not lose messages - duplicates we can't do as much about
            Assert.Equal(publishedMessageCount, consumedMessages.Count);
        });
    }
}